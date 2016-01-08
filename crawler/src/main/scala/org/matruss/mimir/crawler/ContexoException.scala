package org.matruss.mimir.crawler

import akka.event.LoggingAdapter

sealed trait ContexoException
{
  val msg:String
  val cause:Option[Throwable]
  def printError()(implicit logger:LoggingAdapter)
  {
    logger.error("[{}] Error message: {}", Array(this.getClass.getSimpleName, msg) )
    if (cause.isDefined) {
      val ex = cause.get
      logger.error("[{}] Cause: {}", Array(this.getClass.getSimpleName, ex.getMessage) )
      logger.error("[{}] Stack trace: {}", Array(this.getClass.getSimpleName, ex.printStackTrace() ) )
    }
  }
}
case class InputMonitorException(msg:String, cause:Option[Throwable] = None) extends Exception(msg) with ContexoException
case class InputFileReaderException(msg:String, cause:Option[Throwable] = None) extends Exception(msg) with ContexoException
case class CacheBuilderException(msg:String, cause:Option[Throwable] = None) extends Exception(msg) with ContexoException
case class WebHarvesterServiceException(msg:String, cause:Option[Throwable] = None) extends Exception(msg) with ContexoException
case class WebHarvesterException(msg:String, cause:Option[Throwable] = None) extends Exception(msg) with ContexoException
case class ContentFetcherException(msg:String, cause:Option[Throwable] = None) extends Exception(msg) with ContexoException
case class ContentFlushingException(msg:String, cause:Option[Throwable] = None) extends Exception(msg) with ContexoException
