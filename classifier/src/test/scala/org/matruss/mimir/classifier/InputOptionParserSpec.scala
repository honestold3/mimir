package org.matruss.mimir.classifier

import org.scalatest.{BeforeAndAfter, WordSpec}
import scala.collection.mutable.Buffer
import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration

class InputOptionParserSpec extends WordSpec with BeforeAndAfter
{
  val main = new FCRClassifierMapReduce;
  main.setConf(new Configuration);

  "Main class" should {
    val (input, outputDef, outputSpec, rules, cats, job) = ("input", "/out/put/def", "outputspec", "/clas/rules", "/clas/cats","adxLogToText");

    "parse correctly correct arguments and update arguments with input options" in {
      val args = Array[String]("-v","-i",input, "-o", outputDef, "-n", outputSpec, "-r",rules, "-t", cats, "-j", job);

      main.parseArgs(args);
      assert(main.inputPath.head.getName.equalsIgnoreCase( input) )
      assert(main.outputClassifiedPath.get.equalsIgnoreCase(outputDef))
      assert(main.outputUnclassifiedPath.get.getName.equalsIgnoreCase(outputSpec))
      assert(main.rulesFilePath.get.name.equalsIgnoreCase(rules))
      assert(main.categoriesFilePath.get.name.equalsIgnoreCase(cats))
      assert(main.jobToRun.name.equalsIgnoreCase(job))
      assert(main.isVerbose)
    }
    "throw parsing exception if incorrect option" in {
      val args = Array[String]("-v","-i",input, "-o", outputDef, "-n", outputSpec, "-r",rules, "-t", cats, "-j", job, "-z","tada");

      intercept[ParseException] { main.parseArgs(args) }
    }
    "throw no input files exception if input path is empty" in {
      val args = Array[String]("-v","-o", outputDef, "-n", outputSpec, "-r",rules, "-t", cats, "-j", job);

      main.setInputPath(Buffer[Path]());
      intercept[NoMatchingInputFiles] { main.parseArgs(args) }
    }
    "throw no output path exception if either default or specific output path is not set" in {
      val args1 = Array[String]("-v","-i",input,"-n", outputSpec, "-r",rules, "-t", cats, "-j", job);

      main.setOutputClassifiedPath(None);
      intercept[NoOutputFileSpecified] { main.parseArgs(args1) }

      val args2 = Array[String]("-v","-i",input, "-o", outputDef, "-r",rules, "-t", cats, "-j", job);
      main.setOutputUnclassifiedPath(None);
      intercept[NoOutputFileSpecified] { main.parseArgs(args2) }
    }
  }
}
/*
    val parser = new OptionParser("fcr-mapreduce") {
      opt("v", "verbose", "causes program to print elaborate debugging messages through the process of execution", {
        Verbose = true
        Logger.getRootLogger.setLevel(Level.DEBUG)
      })
      opt("i", "input", "Fast classifier input file(s).  Multiple files may be specified as a comma-separated list", {
        v: String => INPUT_PATHS = v.split(',') map(new Path(_)) toBuffer
      })
      opt("o", "classified-output", "Job output path for classified URLs", { v: String => OutClassifiedPath = Some(v) })
      opt("n", "non-classified-output", "Job output path for non-classified URLs", { v: String => OutUnclassifiedPath = Some(new Path(v)) })
      opt("r", "rules", "Location of file with fast classification rules", { v: String => RulesFile = Some(InputSource(ExternalDataLocation,v)) })
      opt("t", "categories", "Location of file with classification categories", { v: String => CategoriesFile = Some(InputSource(ExternalDataLocation,v)) })
      opt("j", "job", "Type of job to run, or 'adxLogToText'. Available jobs are : %s (default: adxLogToText)".format(Jobs.jobTypes.map(_.name).toList.mkString(",")), {
        v: String => JobToRun = Symbol(v)
      })
    }
*/