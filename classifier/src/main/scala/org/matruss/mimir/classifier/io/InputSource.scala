package org.matruss.mimir.classifier.io

import java.io.{FileInputStream, File}
import org.apache.hadoop.fs.{FileSystem, Path}
import java.util.Scanner
import org.apache.commons.io.IOUtils
import java.net.URI

import org.matruss.mimir.classifier.ConfigurationProvider

/**
 * Generic trait defining input source independently of origin.
 * Currently could be local fs, hdfs or avro
 * Almost exactly the same as corresponding piece in content fetcher, need to external it in a library of sort
 */
// todo ! bellow is almost verbatim copy from crawler, should make it a library and use it then
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
  def identity:Symbol
  def uri:URI
  def mkString:Option[String] =
  {
    var res:Option[String] = None
    if (holder.isDefined)
    {
      val builder = new StringBuilder
      while(holder.hasNext) { builder.append(holder.next) }
      res = Some(builder.toString())
    }
    res
  }
}

class LocalFile(val name:String) extends InputSource
{
  type InputEntity = File
  type InputHolder = CustomScanner

  protected class CustomScanner(file:File)
  {
    lazy private[this] val optScanner = {
      try { Some(new Scanner( new FileInputStream(file) ) ) }
      catch { case e:Exception => None }
    }
    lazy private[this] val scanner = if (optScanner.isDefined) optScanner.get else new Scanner("")

    def isDefined:Boolean = optScanner.isDefined
    def next:String = scanner.nextLine
    def hasNext:Boolean = scanner.hasNextLine
    def close() { scanner.close(); }
    def nothing:String = ""
  }

  protected var entity = new File(name)
  val holder = new CustomScanner(entity)

  val identity = InputSymbols.LOCAL_SYM
  def uri:URI = entity.toURI
}

class HadoopFile(val name:String) extends InputSource
{
  type InputEntity = Path
  type InputHolder = CustomDataReader;

  protected class CustomDataReader(path:Path)
  {
    lazy private[this] val optStream = {
      try { Some(InputSource.FS.open(path) ) }
      catch { case e:Exception => {
        // todo change to call to logger
        System.out.println("In InputSource: " + e.getMessage)
        System.out.println("In InputSource: " + e.getStackTraceString)
        None
      } }
    }
    lazy private[this] val stream = if (optStream.isDefined) optStream.get else null
    lazy private[this] val iterator = IOUtils.lineIterator(stream, null)

    def isDefined:Boolean = optStream.isDefined
    def next:String = iterator.next
    def hasNext:Boolean = iterator.hasNext
    def close() { stream.close(); }
  }

  protected var entity = new Path(name)
  val holder = new CustomDataReader(entity)
  def uri:URI = entity.toUri

  val identity = InputSymbols.HDFS_SYM
}

object InputSymbols
{
  val LOCAL_SYM = 'file
  val HDFS_SYM = 'hdfs
}

object InputSource extends InputSourceResolver

trait InputSourceResolver
{
  protected val config = ConfigurationProvider.config
  protected val InputHDFSUri = "hdfs://cluster-ny7"

  lazy val FS = FileSystem.get(URI.create(InputHDFSUri), config)


  import InputSymbols._
  protected val sources = Map[Symbol, String => InputSource](
    LOCAL_SYM -> { name => new LocalFile(name) },
    HDFS_SYM -> { name => new HadoopFile(name) }
  )

  def apply(cls:Symbol, name:String):InputSource =
  {
    sources.get(cls) match {
      case Some(func) => func(name);
      case None => new LocalFile(name)
    }
  }
}

