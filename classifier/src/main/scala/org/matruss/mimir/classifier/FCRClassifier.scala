package org.matruss.mimir.classifier

import io.InputSource
import org.apache.hadoop.util.{Tool, ToolRunner}
import org.apache.hadoop.conf.{Configured, Configuration}
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.{Level, Logger}
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.fs.{FileSystem, Path}
import scala.collection.mutable.Buffer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import scopt.mutable.OptionParser
import sun.font.CreatedFontTracker
import java.net.URI

object FCRClassifier extends App
{
  System.exit(ToolRunner.run(ConfigurationProvider.config, new FCRClassifierMapReduce, args))
}

class FCRClassifierMapReduce extends Configured with Tool
{
  private[this] val logger = Logger.getLogger(getClass)
  import ConfigurationProvider._
  def parseArgs(args:Array[String])
  {
    val parser = new OptionParser("fcr-mapreduce") {
      opt("v", "verbose", "causes program to print elaborate debugging messages through the process of execution", {
        Verbose = true
        Logger.getRootLogger.setLevel(Level.DEBUG)
      })
      opt("i", "input", "Fast classifier input file(s).  Multiple files may be specified as a comma-separated list", {
        v: String => {
          logger.info("input path is set to " + v)
          InputPaths = v.split(',') map(new Path(_)) toBuffer;
          getConf.set("input_path", v)
        }
      })
      opt("o", "classified-output", "Job output path for classified URLs", { v: String => {
        logger.info("output path for classified urls is set to " + v)
        OutClassifiedPath = Some(v)
        getConf.set("classified_output_path", v)
      } })
      opt("n", "non-classified-output", "Job output path for non-classified URLs", { v: String => {
        logger.info("output path for non-classified urls is set to " + v)
        OutUnclassifiedPath = Some(new Path(v))
        getConf.set("non_classified_output_path", v)
      } })
      opt("r", "rules", "Location of file with fast classification rules", { v: String => {
        logger.info("rules file location is set to " + v)
        RulesFile = Some(InputSource(ExternalDataLocation,v))
        getConf.set("rules_file", v)
      } })
      opt("t", "categories", "Location of file with classification categories", { v: String => {
        logger.info("master categories file location is set to " + v)
        CategoriesFile = Some(InputSource(ExternalDataLocation,v))
        getConf.set("categories_file", v)
      } })
      opt("j", "job", "Type of job to run, or 'adxLogToText'. Available jobs are : %s (default: adxLogToText)".format(Jobs.jobTypes.map(_.name).toList.mkString(",")), {
        v: String => JobToRun = Symbol(v)
      })
    }

    // Throw exception on parser or validation error
    if (!parser.parse(args)) throw ParseException("Parse exception")
    if (InputPaths.size == 0) throw NoMatchingInputFiles("No matching input files")
    if (!OutClassifiedPath.isDefined) throw NoOutputFileSpecified("No output file specified")
    if (!OutUnclassifiedPath.isDefined) throw NoOutputFileSpecified("No output file specified")
  }

  // todo ? let it throw Exception for now, might want to re-think it later
  private def validateAll()
  {
    logger.info("validating fast classification rules")

    import ExternalData._
    val areCategoriesNamesValid = FastClassificationRule.validateAgainst(RULES_LIST) (CATEGORIES_LIST)
    logger.info("validation against master categories list: " + areCategoriesNamesValid)

    val areRulesValid = FastClassificationRule.validateSelf(RULES_LIST)
    logger.info("self-validation: " + areRulesValid )

    if(!areCategoriesNamesValid && !areRulesValid) throw ValidationFailed("Validation failed")
  }

  def run(args:Array[String]) : Int =
  {
    try {
      parseArgs(args)
      validateAll()

      val job = Jobs.updateJobFor(JobToRun, new Job( getConf, "fcr_classifier") )

      job.getConfiguration.set("rules_link_name", RULES_LINK)
      val localUri = new URI( RulesFile.get.uri.toString + "#" + RULES_LINK )
      job.addCacheFile(localUri)

      logger.debug("Added : " + job.getCacheFiles.size + " file :" + localUri + " to distributed cache")

      InputPaths foreach { path => FileInputFormat.addInputPath(job, path) }
      FileOutputFormat.setOutputPath(job, OutUnclassifiedPath.get)

      if (System.getenv("HADOOP_TOKEN_FILE_LOCATION") != null) {
        job.getConfiguration.set("mapreduce.job.credentials.binary", System.getenv("HADOOP_TOKEN_FILE_LOCATION"))
      }

      // it is not needed now, but it very well might be needed in the future if HBase will be as one of output options
      //if(User.isHBaseSecurityEnabled(conf)) {
      //  TokenUtil.obtainTokenForJob(conf, UserGroupInformation.getCurrentUser, job)
      //}
      if (!job.waitForCompletion(true)) throw JobFailed("Job failed")

      SUCCESS
    }
    catch {
      case e: Exception => {
        val logger = Logger.getLogger(getClass)
        logger.error("ERROR: " + e.getMessage)
        if (Verbose) logger.debug(e.getMessage, e)
        FAILURE
      }
    }
  }

  // these getters/setters are for testing purpose only
  def inputPath:Buffer[Path] = InputPaths
  def outputClassifiedPath:Option[String] = OutClassifiedPath
  def outputUnclassifiedPath:Option[Path] = OutUnclassifiedPath
  def rulesFilePath:Option[InputSource] = RulesFile
  def categoriesFilePath:Option[InputSource] = CategoriesFile
  def jobToRun:Symbol = JobToRun
  def isVerbose:Boolean = Verbose

  def setInputPath(value:Buffer[Path]) { InputPaths = value }
  def setOutputClassifiedPath(value:Option[String]) { OutClassifiedPath = value }
  def setOutputUnclassifiedPath(value:Option[Path]) { OutUnclassifiedPath = value }
}

object ConfigurationProvider
{
  val config = new Configuration

  config.addResource("application.xml")

  var Verbose = config.get("verbose","false") toBoolean

  var InputPaths = {
    val fromConfig = config.get("input_path","")
    if (fromConfig.size > 0) fromConfig split(',') map(new Path(_)) toBuffer
    else Buffer[Path]()
  }
  var OutClassifiedPath = {
    val fromConfig = config.get("classified_output_path","")
    if (fromConfig.size > 0) Some(fromConfig)
    else None;
  }
  var OutUnclassifiedPath = {
    val fromConfig = config.get("non_classified_output_path","")
    if (fromConfig.size > 0) Some(new Path(fromConfig) )
    else None;
  }
  // todo currently external files must reside on HDFS to be added to distributed cache
  val ExternalDataLocation = Symbol ( config.get("external_data_location", "hdfs"))
  var RulesFile = {
    val fromConfig = config.get("rules_file","")
    if (fromConfig.size > 0) Some(InputSource(ExternalDataLocation, fromConfig))
    else None;
  }
  var CategoriesFile = {
    val fromConfig = config.get("categories_file","")
    if (fromConfig.size > 0) Some(InputSource(ExternalDataLocation, fromConfig))
    else None
  }
  var JobToRun = Symbol ( config.get("job_to_run", "fcrSeqToText") )

  val RULES_LINK = "rules.json"
  val SUCCESS = 0
  val FAILURE = 1
}

object ExternalData
{
  private lazy val categoriesSource = Category.build(ConfigurationProvider.CategoriesFile)
  private lazy val rulesSource = FastClassificationRule.build(ConfigurationProvider.RulesFile)

  // since both categories and rules can be accesses by many mapper instances, it should be access as immutable collection
  lazy val CATEGORIES_LIST = categoriesSource.getOrElse(List[Category]())
  lazy val RULES_LIST = rulesSource.getOrElse(List[FastClassificationRule]())
}