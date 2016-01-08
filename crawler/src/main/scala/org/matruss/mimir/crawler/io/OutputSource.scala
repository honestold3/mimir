package org.matruss.mimir.crawler.io

import org.matruss.mimir.crawler.WContent

import collection.mutable.Set
import java.io.File
import org.apache.hadoop.fs.Path

import org.apache.avro.file.DataFileWriter
import org.apache.avro.specific.SpecificDatumWriter
import java.util.Date
import IOSource.{FS, CODEC_NAME, codec, hadoopConfig}
import akka.event.LoggingAdapter
import org.apache.commons.io.FileUtils
import collection.JavaConversions._
import org.apache.hadoop.io.compress.CompressionOutputStream
import org.apache.hadoop.io.{LongWritable, NullWritable, Text, SequenceFile}
import org.apache.hadoop.io.SequenceFile.CompressionType

/**
 * Generic trait defining output independently of origin.
 * Currently can be local fs, hdfs or avro
*/
 trait OutputSource
{
	def logger:LoggingAdapter
	def name:String
	def dump(contents:Set[WContent]):Boolean
  def flush()
  def close()
}

abstract class LocalOutput(val base:String) extends OutputSource
{
  implicit val logger:LoggingAdapter
  val name = base + "." + new Date().getTime + ".txt"
  private[this] val out = new File(name)

  def dump(contents:Set[WContent]):Boolean =
	{
		logger.debug("[{}] Starting output dump to {}", Array(this.getClass.getSimpleName, name))
    var status = true
		try {
			val res = contents map { entry => entry.toLine }
      FileUtils.writeLines(out, res, "\n", true)
		}
		catch {
			case e:Exception => {
				logger.warning("[{}] Error writing output: {}", Array(this.getClass.getSimpleName, e.toString))
        status = false
			}
		}
    status
	}

  def flush() {}
  def close() {}    // nothing to close for local fs
}

abstract class HadoopOutput(val base:String) extends OutputSource
{
  implicit val logger:LoggingAdapter
  val name = base + "." + new Date().getTime + ".snappy"
  private[this] val (path, handle) = {
    val path = ContexoUtils.toOption { new Path(name) }
    val handle = if (path.isDefined) ContexoUtils.toOption { FS.create(path.get) } else None
    (path, handle)
  }
  if (handle.isDefined) handle.get.close()

  def dump(contents:Set[WContent]):Boolean =
  {
    logger.debug("[{}] Starting output dump to {}", Array(this.getClass.getSimpleName, name))
    var status = path.isDefined && handle.isDefined

    if (status) {
      val out = ContexoUtils.toOption { FS.append( path.get ) } getOrElse(null)
      var outStream:Option[CompressionOutputStream] = None
      try {
        val cd = codec.getCodecByName(CODEC_NAME)
        outStream = Some(cd.createOutputStream(out))
        contents foreach { entry =>
          outStream.get.write(entry.toLine.append('\n').toString.getBytes("UTF-8"))
        }
      }
      catch {
        case e:Exception => {
          logger.warning("[{}] Error writing output: {}", Array(this.getClass.getSimpleName, e.toString))
          status = false
        }
      }
      finally { if(outStream.isDefined) outStream.get.close() }
    }
    status
  }

  def flush() { if (handle.isDefined) handle.get.close() }
  def close() { flush() }
}

abstract class SequenceOutput(val base:String) extends OutputSource
{
  implicit val logger:LoggingAdapter
  val name = base + "." + new Date().getTime + ".seq.snappy"

  private[this] val defaultKey = new LongWritable(1)
  private[this] val valueCls = (new Text).getClass
  private[this] val (path, handle) = {
    val path = ContexoUtils.toOption { new Path(name) }
    val handle = if (path.isDefined) {
      val cd = codec.getCodecByName(CODEC_NAME)
      ContexoUtils.toOption { SequenceFile.createWriter(FS, hadoopConfig, path.get, defaultKey.getClass, valueCls, CompressionType.BLOCK, cd) }
    } else None
    (path, handle)
  }

  def dump(contents:Set[WContent]):Boolean =
  {
    logger.debug("[{}] Starting output dump to {}", Array(this.getClass.getSimpleName, name))
    var status = path.isDefined && handle.isDefined

    if (status) {
      try {
        contents foreach { entry => handle.get.append(defaultKey, new Text(entry.toLine.toString)) }
      }
      catch {
        case e:Exception => {
          logger.warning("[{}] Error writing output: {}", Array(this.getClass.getSimpleName, e.toString))
          status = false
        }
      }
      finally { flush() }
    }
    status
  }

  def flush() { if (handle.isDefined) handle.get.hflush() }
  def close() { if (handle.isDefined) handle.get.close() }
}

abstract class AvroOutput(val base:String) extends OutputSource
{
  implicit val logger:LoggingAdapter
  val name:String = base + "." + new Date().getTime + ".avro.snappy"

  private[this] val (path, handle) = {
    val path = ContexoUtils.toOption { new Path(name) }
    val handle = if (path.isDefined) ContexoUtils.toOption { FS.create(path.get) } else None
    (path, handle)
  }
  if (handle.isDefined) handle.get.close()

  def dump(contents:Set[WContent]):Boolean =
	{
		logger.debug("[{}] Starting output dump to {}", Array(this.getClass.getSimpleName, name))
    var status = path.isDefined && handle.isDefined

		if (status) {
      val out = ContexoUtils.toOption { FS.append( path.get ) } getOrElse(null)
      var stream:Option[CompressionOutputStream] = None
      var writer:Option[DataFileWriter[RawWebContent]] = None

      try {
        val cd = codec.getCodecByName(CODEC_NAME)
        stream = Some(cd.createOutputStream(out))
        writer = Some(new DataFileWriter[RawWebContent]( new SpecificDatumWriter[RawWebContent]() ).create(RawWebContent.SCHEMA$, stream.get))
        for (entry <- contents) {
          val datum = new RawWebContent
          if (entry.content.isDefined) {
            val cleaned = entry.clean
            datum.setUrl(cleaned.content.get._1)
            datum.setContent(cleaned.content.get._2)
            datum.setCount(cleaned.content.get._3)
            writer.get.append(datum)
          }
        }
      }
      catch {
        case e:Exception => {
          logger.warning("[{}] Error writing output: {}", Array(this.getClass.getSimpleName, e.toString))
          status = false
        }
      }
      finally {
        writer.get.close()
        out.close()
      }
    }
		status
	}

  def flush() {}
  def close() {}
}

object OutputSource
{
  import IOConstants._
  private val sources = Map[Symbol, String => OutputSource](
    LOCAL_SYM -> { name => new LocalOutput(name) { implicit val logger = ConfigurationProvider.zoo.log }},
    HDFS_SYM -> { name => new HadoopOutput(name) { implicit val logger = ConfigurationProvider.zoo.log }},
    AVRO_SYM -> { name => new AvroOutput(name) { implicit val logger = ConfigurationProvider.zoo.log }},
    SEQ_SYM -> { name => new SequenceOutput(name) { implicit val logger = ConfigurationProvider.zoo.log }}
  )

  def apply(cls:Symbol, name:String):OutputSource =
  {
    sources.get(cls) match {
      case Some(func) => func(name)
      case None => new LocalOutput(name) { implicit val logger = ConfigurationProvider.zoo.log }
    }
  }
}
