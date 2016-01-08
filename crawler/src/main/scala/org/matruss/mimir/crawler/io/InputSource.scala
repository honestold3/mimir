package org.matruss.mimir.crawler.io

import collection.mutable.Buffer
import io.IOConstants
import io.IOSource.FS
import java.io.{FileInputStream, File}
import org.apache.hadoop.fs.Path
import java.util.Scanner
import org.apache.commons.io.IOUtils
import org.apache.avro.file.DataFileReader
import org.apache.avro.mapred.FsInput
import org.apache.avro.specific.SpecificDatumReader
import akka.event.LoggingAdapter

/**
 * Generic trait defining input source independently of origin.
 * Currently could be local fs, hdfs or avro
 */
trait InputSource
{
	type InputEntity
	type InputHolder <: {
		def isDefined:Boolean
		def next:String
		def hasNext:Boolean
		def close()
	}

	protected def entity:InputEntity
	val holder:InputHolder           // todo ? might need to make it lazy

	def name:String
	def list:Seq[String]
	def renameTo(name:String):Boolean
	def identity:Symbol
}

abstract class LocalFile(val name:String) extends InputSource
{
	type InputEntity = File
	type InputHolder = CustomScanner

	implicit val logger:LoggingAdapter

	protected class CustomScanner(file:File)
	{
		lazy private[this] val optScanner = ContexoUtils.toOption { new Scanner( new FileInputStream(file) ) }
		lazy private[this] val scanner = if (optScanner.isDefined) optScanner.get else new Scanner("")

		def isDefined:Boolean = optScanner.isDefined
		def next:String = scanner.nextLine
		def hasNext:Boolean = scanner.hasNextLine
		def close() { scanner.close() }
		def nothing:String = ""
	}

	protected var entity = new File(name)
	val holder = new CustomScanner(entity)

	def list:Buffer[String] =
	{
		val files = Buffer[String]()
		if (entity.isFile) files += entity.getName
		else
		{
			try {
        // todo don't need that check for DONE any more
				val roster = entity.listFiles() filter {file => !file.getName.endsWith("DONE")} map {file => file.getAbsolutePath} toBuffer;
				files ++= roster
			}
			catch {
				case e:Exception => logger.warning("[{}] Error while creating a list of input files: {}", Array(this.getClass.getSimpleName, e.toString))
			}
		}
		files
	}

	def renameTo(newName:String):Boolean = entity.renameTo( new File(newName) )
	val identity = IOConstants.LOCAL_SYM
}

abstract class HadoopFile(val name:String) extends InputSource
{
	type InputEntity = Path
	type InputHolder = CustomDataReader

	implicit val logger:LoggingAdapter

	protected class CustomDataReader(path:Path)
	{
		lazy private[this] val optStream = {
			val value = ContexoUtils.toOption { FS.open(path) }
			logger.debug("[{}] Created stream for {}", Array(this.getClass.getSimpleName, name))
			value
		}
		lazy private[this] val stream = if (optStream.isDefined) optStream.get else null  // todo ! zoot bad, bad
		lazy private[this] val iterator = IOUtils.lineIterator(stream, null)

		def isDefined:Boolean = optStream.isDefined
		def next:String = iterator.next
		def hasNext:Boolean = iterator.hasNext
		def close() { stream.close() }
	}

	protected var entity = new Path(name)
	val holder = new CustomDataReader(entity)

	def list:Buffer[String] =
	{
		val files = Buffer[String]()
		try {
			val roster:Buffer[String] = FS.listStatus(entity) map {status => status.getPath.toUri.getPath} filter {file => !file.endsWith("DONE")} toBuffer;
			files ++= roster
		}
		catch {
			case e:Exception => logger.warning("[{}] Error while creating a list of input files: {}", Array(this.getClass.getSimpleName, e.toString))
		}
		files
	}

	def renameTo(newName:String):Boolean = FS.rename(entity, new Path(newName))
	val identity = IOConstants.HDFS_SYM
}

abstract class AvroDatum(val name:String) extends InputSource
{
	type InputEntity = Path
	type InputHolder = CustomAvroReader

	implicit val logger:LoggingAdapter

	protected class CustomAvroReader(path:Path)
	{
		lazy private[this] val optReader = ContexoUtils.toOption {
			new DataFileReader[TargetURL] (
				new FsInput(path, InputSource.config ),
				new SpecificDatumReader[TargetURL]()
			)
		}
		lazy private[this] val reader = if (optReader.isDefined) optReader.get else null  // todo ! zoot bad, bad
		lazy private[this] val iterator = reader.iterator

		def isDefined:Boolean = optReader.isDefined
		def next:String =
		{
			val line = iterator.next()
			val buffer = new StringBuffer
			buffer.append(line.getUrl).append('\t').append(line.getCount).toString
		}
		def hasNext:Boolean = iterator.hasNext
		def close() { reader.close() }
	}

	protected var entity = new Path(name)
	val holder = new CustomAvroReader(entity)

	def list:Buffer[String] =
	{
		val files = Buffer[String]()
		try {
			val roster:Buffer[String] = FS.listStatus(entity) map {status => status.getPath.getName} filter {file => !file.endsWith("DONE")} toBuffer;
			files ++= roster
		}
		catch {
			case e:Exception => logger.warning("[{}] Error while creating a list of input files: {}", Array(this.getClass.getSimpleName, e.toString))
		}
		files
	}

	def renameTo(newName:String):Boolean = FS.rename(entity, new Path(newName))
	val identity = IOConstants.AVRO_SYM
}

object InputSource
{
	val config = ConfigurationProvider.hadoopConfig

	import IOConstants._
	private val sources = Map[Symbol, String => InputSource](
		LOCAL_SYM -> { name => new LocalFile(name) { implicit val logger = ConfigurationProvider.zoo.log }},
		HDFS_SYM -> { name => new HadoopFile(name) { implicit val logger = ConfigurationProvider.zoo.log }},
		AVRO_SYM -> { name => new AvroDatum(name) { implicit val logger = ConfigurationProvider.zoo.log }}
	)

	def apply(cls:Symbol, name:String):InputSource =
	{
		sources.get(cls) match {
			case Some(func) => func(name)
			case None => new LocalFile(name) { implicit val logger = ConfigurationProvider.zoo.log }
		}
	}
}
