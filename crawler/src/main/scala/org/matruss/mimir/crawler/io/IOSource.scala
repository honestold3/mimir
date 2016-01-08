package org.matruss.mimir.crawler.io

import org.apache.hadoop.fs.FileSystem
import java.net.URI
import org.apache.hadoop.io.compress.CompressionCodecFactory

object IOSource
{
	val akkaConfig = ConfigurationProvider.akkaConfig
	val section = ConfigurationProvider.section
	val hadoopConfig = ConfigurationProvider.hadoopConfig
	implicit val logger = ConfigurationProvider.zoo.log

	// todo this better be sourced from external hadoop config
	val INPUT_HDFS_URI = toOption { akkaConfig.getString(section + ".io.input_hdfs_uri") } getOrElse("hdfs://localhost")
	val CODEC_NAME = toOption { akkaConfig.getString(section + ".io.codec_name") } getOrElse("SnappyCodec")

	lazy val FS = FileSystem.get(URI.create(INPUT_HDFS_URI), hadoopConfig)
	lazy val codec = new CompressionCodecFactory(hadoopConfig)
}

object IOConstants
{
	val LOCAL_SYM = 'local
	val HDFS_SYM = 'hdfs
	val AVRO_SYM = 'avro
  val SEQ_SYM = 'seq
}

