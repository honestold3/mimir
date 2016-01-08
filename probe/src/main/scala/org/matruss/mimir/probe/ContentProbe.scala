package org.matruss.mimir.probe

import akka.actor.Actor
import org.apache.http.client.methods.HttpGet
import org.apache.http.util.EntityUtils
import org.apache.http.client.{ClientProtocolException, HttpClient}
import java.util.concurrent.TimeUnit
import java.io.IOException
import WebProbe.{HTTP_PROTOCOL_ERROR, IO_ERROR, CONNECTION_ERROR, REQUEST_ERROR, UNKNOWN_ERROR}

/*
 Worker actor which probes individual URL, takes its content, sanitize it, then send collected information to dispatcher
*/
class ContentProbe extends Actor
{
  private val logger = context.system.log

  def receive:Receive =
  {
    case target:TargetURL =>
    {
      val url = target.url
      val client = target.client
      logger.debug("[{}] Processing url: {}", Array(this.getClass.getSimpleName, url))
      val request = createRequest(url)
      try {
        val (code, content) = execute(request, client)

        val line = toLine(target.url, content.getBytes.size, code)
        logger.debug("[{}] About to send back a line: {}", Array(this.getClass.getSimpleName, line))

        sender ! ProbeDone(line)
      }
      catch {
        case e: ClientProtocolException => {
          logger.debug("[{}] HTTP protocol error for url {} : becasue of {} : {}", Array(this.getClass.getSimpleName, url, e.getClass.getSimpleName, e.getMessage))
          val line = toLine(url, 0, HTTP_PROTOCOL_ERROR)

          sender ! ProbeDone(line)
        }
        case e: IOException => {
          logger.debug("[{}] IO failed for url {} : becasue of {} : {}", Array(this.getClass.getSimpleName, url, e.getClass.getSimpleName, e.getMessage))
          val line = toLine(url, 0, IO_ERROR)

          sender ! ProbeDone(line)
        }
        case e: Exception => {
          logger.debug("[{}] Failed to get conetent for url {} : becasue of {} : {}", Array(this.getClass.getSimpleName, url, e.getClass.getSimpleName, e.getMessage))
          val line = toLine(url, 0, CONNECTION_ERROR)

          sender ! ProbeDone(line)
        }
      }
      finally {
        if (request.isDefined) request.get.releaseConnection()
        client.getConnectionManager.closeExpiredConnections()
        client.getConnectionManager.closeIdleConnections(5000, TimeUnit.MILLISECONDS )
      }
    }
    case x =>
    {
      logger.warning("[{}] Unknown message for the worker", Array(this.getClass.getSimpleName))
      val line = toLine("", 0, UNKNOWN_ERROR)

      sender ! ProbeDone(line)
    }
  }

  private def createRequest(url:String):Option[HttpGet] =
  {
    try { Some( new HttpGet( url) ) }
    catch {
      case e:IllegalArgumentException => {
        logger.warning("[{}] Parsing url {} to create URI failed becasue of {} : {}", Array(this.getClass.getSimpleName, url, e.getClass.getSimpleName, e.getMessage))
        None
      }
      case e:Exception => {
        logger.warning("[{}] Building URI for url {} failed: becasue of {} : {}", Array(this.getClass.getSimpleName, url, e.getClass.getSimpleName, e.getMessage))
        None
      }
    }
  }

  private def execute(req:Option[HttpGet], client:HttpClient):Pair[Int,String] =
  {
    var res = Pair(REQUEST_ERROR,"")
    if (req.isDefined) {
      val response = client.execute(req.get)
      val code = response.getStatusLine.getStatusCode
      val entity = response.getEntity
      if (code < 300) res = Pair( code, sanitize( EntityUtils.toString(entity) ) )
      EntityUtils.consume(entity)
    }
    res
  }

  private def sanitize(content:String):String =
  {
    import WebProbe.{P_MS, P_NP}

    val page = P_NP.matcher(content).replaceAll(" ").trim
    P_MS.matcher(page).replaceAll(" ")
  }

  private def toLine(url:String,size:Long,code:Int):String =
  {
    val line = new StringBuilder(url)
    line.append(',').append(size).append(',').append(code).append('\n').toString()
  }
}
