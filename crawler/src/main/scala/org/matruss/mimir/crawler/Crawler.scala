package org.matruss.mimir.crawler

import akka.actor._
import collection.mutable.Buffer
import akka.pattern.ask
import org.matruss.mimir.crawler.io.{InputSource, OutputSource}
import java.util.Date
import com.typesafe.config.{ConfigFactory, Config}
import collection.JavaConversions._
import org.apache.hadoop.conf.Configuration
import akka.util.Timeout
import akka.util.duration._

/**
 * Main class, possible options can be:
 *  -v : verbose (doesn't currently work, should be hooked with Akka's log level switch)
 *  -i <directory> | <file> : either input directory to pick up all the files there, or just one file
 *  -o <directory> : output directory
 *
 *  All options are ot mandatory and can be supplied instead in application.conf
 *
 *  This class works by starting all the actors, and creating a future which encompasses all application's work
 *  (which means timeout should be reasonably long)
 */
object ContentFetcher extends App
{
	import ConfigurationProvider._
	implicit val logger = zoo.log
  implicit val timeout:Timeout = Timeout(KickstarterTimeout seconds)
  private val terminationTimeout = Timeout(TERMINATION_TIMEOUT milliseconds)

  if (!isValidConfig(akkaConfig, section)) lastRites(1, "[{}] Invalid configuration file, shutting down actor system and exiting ...")
  if (!parseArgs(args)) lastRites(1, "[{}] Failed to aquire input parameters, shutting down actor system and exiting ...")

	val eventBus = new ApplicationEventBus { implicit val logger = zoo.log}

  // initial kick starter
  val starter = zoo.actorOf(Props( new KickStarter(eventBus) ), name = "KickStarter")

	// top-level actors
	zoo.actorOf(Props( new InputFileMonitor(eventBus) ), name = "InputFileMonitor" )
	// todo  there should be few cache builders, as it involves talking to the void
	zoo.actorOf(Props( new CacheBuilder(eventBus) ), name = "CacheBuilder")
	zoo.actorOf(Props( new WebHarvester(eventBus)), name = "WebHarvester")
  zoo.actorOf(Props( new ContentFlushing(eventBus)), name = "ContentFlusher")

  // scan input only once, as it will be re-submitted to scan by Oozie with set frequency
  val done = starter ? ScanInput( new Date() )
  done onSuccess {
    case _ => lastRites(0, "[{}] Finishing successfully, exiting ...")
  }
  done onFailure {
    case e:ContexoException => {
      e.printError()(logger)
      lastRites(1, "[{}] Finishing with error, exiting ...")
    }
    case e:Exception => lastRites(1, "[{}] Finishing with error: " + e.getMessage + ", exiting ...")
    case x => lastRites(1, "[{}] Finishing with error: " + x.toString + ", exiting ...")
  }

  private def lastRites(code:Int, message:String)
  {
    OutputPath.close()
    zoo.shutdown()
    if (code > 0) logger.error(message, Array(this.getClass.getSimpleName) )
    else logger.info(message, Array(this.getClass.getSimpleName) )

    zoo.awaitTermination(terminationTimeout.duration)
    System.exit(code)
  }

  private def parseArgs(args:Array[String]):Boolean =
  {
    val parser = new scopt.mutable.OptionParser("contexo-contentfetcher") {
      opt("v","verbose","causes program to print elaborate debugging messages through the process of execution", { Verbose = true })
      opt("i","input","List of directory(s) to poll for input or files to process.  Multiple files may be specified as a comma-separated list", {
        list =>
          InputPaths.clear()
          list.split(",") foreach { path =>
            logger.info("[{}] Override input path value from command line {}", Array(this.getClass.getSimpleName,path))
            InputPaths += InputSource(InputType, path)
          }
      })
      opt("o","output","Output directory", { path =>
        logger.info("[{}] Override output path value from command line {}", Array(this.getClass.getSimpleName,path))
        OutputPath = OutputSource(OutputType, path + "/" + BaseOutputName)
      })
    }
    val isParsed = parser.parse(args)
    val hasInputPath = (InputPaths.size > 0)

    if (!isParsed)
      logger.error("[{}] Failed to parse input parameters", Array(this.getClass.getSimpleName))
    if (!hasInputPath)
      logger.error("[{}] Empty input, nothing to process", Array(this.getClass.getSimpleName))

    isParsed && hasInputPath
  }
}

object ConfigurationProvider
{
	val section:String = "contexo.content-fetcher"
	val akkaConfig:Config = ConfigFactory.load()
	val zoo:ActorSystem = ActorSystem( "content-fetcher", akkaConfig )
	val hadoopConfig:Configuration = new Configuration    // todo later resources will have to be added
	implicit val logger = zoo.log

	var Verbose = toOption {akkaConfig.getBoolean(section + ".general.verbose") } getOrElse(false)

	val InputType = Symbol (toOption { akkaConfig.getString(section + ".io.input_type") } getOrElse("hdfs") )
	var InputPaths = {
		toOption {
			akkaConfig.getStringList(section + ".io.input_path")
		} match {
			case Some(value) => {
				value foreach { path => logger.info("[{}] Input path value from config {}", Array(this.getClass.getSimpleName,path)) }
				value map { path => InputSource(InputType, path)}
			}
			case None => Buffer[InputSource]()
		}
	}

  val BaseOutputName = toOption { akkaConfig.getString(section + ".io.base_output_name") } getOrElse( "content-fetcher.out")

  val OutputType = Symbol (toOption { akkaConfig.getString(section + ".io.output_type") } getOrElse("hdfs") )
	var OutputPath = {
    toOption { akkaConfig.getString(section + ".io.output_path")
    } match {
      case Some(value) => OutputSource(OutputType, value + "/" + BaseOutputName)
      case None => OutputSource(OutputType, "./" + BaseOutputName)
    }
  }
  val OutputChunkSize = toOption { akkaConfig.getInt(section + ".io.chunk_size_to_flush") } getOrElse(5000)

  var KickstarterTimeout:Long = toOption { akkaConfig.getLong(section + ".timeout.kick_starter") } getOrElse(1000) // seconds
  val TERMINATION_TIMEOUT = 2000 // milliseconds
  var KEEP_WORKING = true
}
