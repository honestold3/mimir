package org.matruss.mimir.classifier.io

import com.codahale.jerkson.Json.parse
import org.apache.log4j.Logger

/**
 * Generic trait defining object building with JSON input
 */
trait ExternalData
{
  type Value
  type ValueSeq <: {
    def elems:List[Value]
  }
  protected def logger = Logger.getLogger(getClass)

  def build(source:Option[InputSource] )(implicit mf:Manifest[ValueSeq]):Option[List[Value]]=
  {
    if (source.isDefined)
    {
      val optString = source.get.mkString
      if (optString.isDefined) {
        try { Some(parse[ValueSeq](optString.get).elems) }
        catch { case e:Exception => {
          logger.warn("Parsing error while processing json source:" + e.getMessage)
          logger.warn("Parsing error while processing json source:" + e.getStackTraceString)
          None
        } }
      }
      else {
        logger.warn("Can not convert JSON source to string")
        None
      }
    }
    else {
      logger.warn("JSON source is undefined")
      None
    }
  }

  // used in testing
  def build(source:String)(implicit mf:Manifest[ValueSeq]):List[Value] = {
    try { parse[ValueSeq](source).elems }
    catch { case e:Exception => List[Value]() }
  }
}
