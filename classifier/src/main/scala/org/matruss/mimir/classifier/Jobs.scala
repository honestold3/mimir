package org.matruss.mimir.classifier

import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{SequenceFileInputFormat, TextInputFormat}
import org.apache.hadoop.io.{MapWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat

object Jobs
{
  val jobs = Map[Symbol, Job => Job](
    'fcrTextToText -> { (job:Job) =>
    job.setJarByClass(classOf[TextKeyMapper])
    job.setInputFormatClass(classOf[TextInputFormat])
    job.setMapperClass(classOf[TextKeyMapper])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    MultipleOutputs.addNamedOutput( job,
                                    "classified",
                                    classOf[TextOutputFormat[Text, MapWritable]],
                                    classOf[Text],
                                    classOf[MapWritable]
    )
    job;
  },
    'fcrSeqToText -> { (job:Job) =>
    job.setJarByClass(classOf[TextKeyMapper])
    job.setInputFormatClass(classOf[SequenceFileInputFormat[Text,NullWritable]])
    job.setMapperClass(classOf[TextKeyMapper])
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[NullWritable])
    MultipleOutputs.addNamedOutput( job,
                                    "classified",
                                    classOf[TextOutputFormat[Text, MapWritable]],
                                    classOf[Text],
                                    classOf[MapWritable]
    )
    job;
    }
  )
  def jobTypes = Seq('fcrTextToText, 'fcrSeqToText, 'fcrTextToAvro)

  def updateJobFor(jobId:Symbol,baseJob:Job):Job = {
    jobs(jobId)(baseJob)
  }
}
