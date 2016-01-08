package org.matruss.mimir.probe

import collection.mutable.Buffer
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import java.io._
import org.apache.commons.io.IOUtils
import java.util.regex.Pattern
import java.util.Date
import com.typesafe.config.{ConfigFactory, Config}
import akka.actor.{Props, ActorSystem}
import akka.pattern.{ask, pipe}
import akka.dispatch.Future
import akka.util.Timeout
import akka.util.duration._
import org.apache.http.client.HttpClient

trait ProbeConfig
{
  var INPUT_PATHS = Buffer[Path]()
  var OUTPUT_PATH = "."
  val P_NP = Pattern.compile("[^\\p{Print}]")
  val P_MS = Pattern.compile(" +")

  val hadoopConfig = new Configuration

  val akkaConfig:Config = ConfigFactory.load()
  val zoo:ActorSystem = ActorSystem( "content-fetcher", akkaConfig )

  val CHUNK_SIZE = akkaConfig.getInt("contexo.web-probe.io.chunk_size_to_flush")
  val HDFS_URI = akkaConfig.getString("contexo.web-probe.io.input_hdfs_uri")
  val ACTOR_POOL = akkaConfig.getInt("contexo.web-probe.actors.probe_pool")
  val FUTURE_TIMEOUT = akkaConfig.getInt("contexo.web-probe.actors.future_timeout")
  val TERMINATION_TIMEOUT = Timeout(2000 milliseconds)

  // HTTP settings
  val SOCKET_TIMEOUT = akkaConfig.getInt("contexo.web-probe.http.socket_timeout")
  val CONNECTION_TIMEOUT = akkaConfig.getInt("contexo.web-probe.http.connection_timeout")
  val CONNECTION_POOL_MAX = akkaConfig.getInt("contexo.web-probe.http.connection_pool_max")
  val CONNECTION_ROUTE_MAX = akkaConfig.getInt("contexo.web-probe.http.connection_route_max")
  val STALE_CONNECTION_CHECK = akkaConfig.getBoolean("contexo.web-probe.http.stale_connection_check")
  val TCP_NODELAY = akkaConfig.getBoolean("contexo.web-probe.http.tcp_nodelay")

  // Error codes
  val HTTP_PROTOCOL_ERROR = -1
  val IO_ERROR = -2
  val CONNECTION_ERROR = -3
  val REQUEST_ERROR = -4
  val UNKNOWN_ERROR = -5

  implicit val timeout:Timeout = Timeout(FUTURE_TIMEOUT seconds)
  implicit val dispatcher = zoo.dispatcher
  val logger = zoo.log
}

case class TargetURL(url:String, client:HttpClient)
case class ProbeDone(stat:String)
case class SliceURL(urls:Buffer[String], index:Int)
case class FlushDone(status:String = "OK")

/*
 This is a main class for the Web Probe, it reads in urls from log processor output, slice it to smaller pieces, then
 probe one slice of data at a time. After each slice is processed, it pipes results to an actor which flush it to a disc file.
 Currently, results being collected from web pages are: their sizes if page retrieval was successful, or error code otherwise.
*/
object WebProbe extends App with ProbeConfig
{
  if (!parseArgs(args)) System.exit(1)
  val name = OUTPUT_PATH + "/" + "ContenFetcherMemoryProbe." + new Date().getTime + ".csv"

  val probeDispatcher = zoo.actorOf(Props(new ProbeDispatcher), name = "ProbeDispatcher")
  val flushService = zoo.actorOf(Props(new FlushService(name)), name = "FlushService")

  logger.info("[{}] Getting urls to probe from HDFS ...", Array(this.getClass.getSimpleName))
  val FS = FileSystem.get(URI.create(HDFS_URI), hadoopConfig)

  val urls = parseInput
  logger.info("[{}] URLs to probe: {}", Array(this.getClass.getSimpleName, urls.size))

  val slices = dissect(CHUNK_SIZE)
  logger.info("[{}] Sliced to {} chunks for periodic flushing to disk", Array(this.getClass.getSimpleName, slices.size))

  val done = Future.sequence(
    slices map { slice =>
      probeDispatcher ? SliceURL(slice._1, slice._2) mapTo manifest[Buffer[ProbeDone]] pipeTo flushService mapTo manifest[Buffer[FlushDone]]
    }
  )
  done onSuccess {
    case res:Buffer[Buffer[FlushDone]] => lastRites(0)
    case x => {
      logger.error("[{}] Something wrong happened, unexpected result: {}", Array(this.getClass.getSimpleName, x.getClass))
      lastRites(1)
    }
  }
  done onFailure {
    case e:Exception => {
      logger.error("[{}] Something wrong happened: {}", Array(this.getClass.getSimpleName,e.getMessage))
      e.printStackTrace()
      lastRites(1)
    }
  }

  private def dissect(sliceSize:Int):Buffer[Pair[Buffer[String],Int]] =
  {
    var count = 0
    urls grouped(sliceSize) map { slice =>
      val res = Pair(slice, count)
      count += 1
      res
    } toBuffer
  }

  private def lastRites(code:Int)
  {
    if (code > 0) logger.info("[{}] Finishing with error", Array(this.getClass.getSimpleName))
    else   logger.error("[{}] Finishing successfully", Array(this.getClass.getSimpleName))

    zoo.shutdown()

    zoo.awaitTermination(TERMINATION_TIMEOUT.duration)
    System.exit(code)
  }

  private def parseArgs(args:Array[String]):Boolean =
  {
    val parser = new scopt.mutable.OptionParser("contexo-contentfetcher") {
      opt("i","input","List of directory(s) to poll for input or files to process.  Multiple files may be specified as a comma-separated list", {
        list =>
          INPUT_PATHS.clear()
          list.split(",") foreach { path => INPUT_PATHS += new Path(path) }
      })
      opt("o","output","Output directory", { path =>
        OUTPUT_PATH = path
      })
    }
    val isParsed = parser.parse(args)
    val hasInputPath = (INPUT_PATHS.size > 0)

    isParsed && hasInputPath
  }

  private def parseInput:Buffer[String] =
  {
    System.out.println("Parsing input ...")
    // parse input from HDFS and collect all the URLs to probe
    INPUT_PATHS map  { path =>
      var optStream:Option[DataInputStream] = None
      val urls = Buffer[String]()
      try {
        optStream = Some( FS.open(path) )
        val iter = IOUtils.lineIterator(optStream.get, null)
        while(iter.hasNext) { urls += iter.next().split('\t').head.trim }
      }
      catch { case e:Exception => } // do nothing, keep going
      finally { if (optStream.isDefined) optStream.get.close()  }
      urls
    } flatten
  }
}
