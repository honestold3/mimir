package org.matruss.mimir.crawler.services

import akka.actor.Actor
import java.io._
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.HttpClient
import org.apache.http.util.EntityUtils
import java.util.regex.Pattern
import java.net.URI
import java.util.NoSuchElementException

import org.matruss.mimir.crawler.WTarget

/**
 * Actual actor which goes and fetches url's content
 */
class WebHarvesterService extends Actor
{
	private[this] val logger = context.system.log

	def receive:Receive =
	{
		case target:WTarget =>
		{
			val (url, counter) = (target.base._1, target.base._2)
      val client = target.client
      val request = createRequest(url)

      logger.debug("[{}] Collecting URLs content from {}", Array(this.getClass.getSimpleName,url))
			try {
				val response = execute(request, client)
				val result = Some(Triple(url,response,counter))
				sender ! WContent(result)
			}
			catch {
				case e: FileNotFoundException => {
					logger.debug("[{}] FileNotFoundException: could not find anything for {} : {}", Array(this.getClass.getSimpleName, url, e.toString))
					sender ! WebHarvesterService.emptyResult(url, counter)
				}
				case e: IOException => {
					logger.info("[{}] IOException: error while trying to fetch from {} : {}", Array(this.getClass.getSimpleName, url, e.toString))
					sender ! WebHarvesterService.emptyResult(url, counter)
				}
				case e: Exception => {
					logger.debug("[{}] Error while trying to reach {} : {}", Array(this.getClass.getSimpleName, url, e.toString))
					sender ! WebHarvesterService.emptyResult(url, counter)
				}
			}
		}
		case _ => {
			logger.warning("[{}] Unknown message", Array(this.getClass.getSimpleName))
			sender ! None
		}
	}

  private def createRequest(url:String):Option[HttpGet] =
  {

    try { Some( new HttpGet( url ) ) }
    catch {
      case e:IllegalArgumentException => {
        logger.info("[{}] Parsing url {} to create URI failed, re-trying with sanitizing it ...", Array(this.getClass.getSimpleName, url))

        val cleaned = WebHarvesterService.sanitize(url)
        if (cleaned.isDefined) {
          try { Some( new HttpGet(cleaned.get) ) }
          catch { case e:Exception => {
              logger.warning("[{}] Building URI for url {} failed: because of {} : {}", Array(this.getClass.getSimpleName, url, e.getClass.getSimpleName, e.getMessage))
              None
            }
          }
        }
        else {
          logger.warning("[{}] Parsing url {} to create URI failed because of {} : {}", Array(this.getClass.getSimpleName, url, e.getClass.getSimpleName, e.getMessage))
          None
        }
      }
      case e:Exception => {
        logger.warning("[{}] Building URI for url {} failed: because of {} : {}", Array(this.getClass.getSimpleName, url, e.getClass.getSimpleName, e.getMessage))
        None
      }
    }
  }

  private def execute(req:Option[HttpGet], client:HttpClient):String =
  {
    var res = ""
    if (req.isDefined) {
      val response = client.execute(req.get)
      val code = response.getStatusLine.getStatusCode
      val entity = response.getEntity
      if (code < 300) res = EntityUtils.toString(entity,"UTF-8") // todo we might want to store response code for further analysis
      EntityUtils.consume(entity)
    }
    res
  }
}

object WebHarvesterService
{
  // The only way to escape illegal characters is to break string into pieces, then create URI using "long" constructor
  def sanitize(url:String):Option[URI] = {
    val iBegin = { if( (url.indexOf("//") > 0) ) (url.indexOf("//") + 2) else 0  }

    // it could potentially be "https", don't want to miss it
    val scheme = { if (url.indexOf("//") > 0) url.substring(0, url.indexOf("//") - 1) else "http" }
    val local = new StringBuilder(url.substring(iBegin))
    if (local.length > 0) {
      val (iPath, iQuery, iFragment) = ( local.indexOf('/'), local.indexOf('?'), local.indexOf('#') )

      val host = {
        if (iPath > 0) local.subSequence(0,iPath).toString else local.toString()
      }
      val path = {
        if (iPath > 0 ) {
          if (iQuery > 0) local.subSequence(iPath, iQuery).toString else local.subSequence(iPath,local.length).toString
        }
        else null
      }
      val query = {
        if (iQuery > 0) {
          if (iFragment > 0) local.subSequence(iQuery + 1, iFragment).toString else local.subSequence(iQuery + 1,local.length).toString
        }
        else null
      }
      val fragment = {
        if (iFragment > 0) local.subSequence(iFragment + 1, local.length).toString else null
      }
      Some( new URI(scheme, host, path, query, fragment) )
    }
    else None
  }

	protected def emptyResult(url:String, counter:Int) = WContent(Some(Triple(url,"",counter)))
}

