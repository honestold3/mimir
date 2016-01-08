package org.matruss.mimir.crawler.fixture

import akka.actor.{Status, Actor}
import spray.http._
import spray.http.HttpResponse
import java.io.IOException
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.conn.scheme.{PlainSocketFactory, Scheme, SchemeRegistry}
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.client.HttpClient
import org.apache.http.params.{CoreConnectionPNames, BasicHttpParams}
import org.apache.http.impl.client.DefaultHttpClient

class MockHttpService extends Actor
{
	import MockHttpService._
	def receive:Receive =
	{
		case HttpRequest(HttpMethods.GET,"/",_,_,_) =>  sender ! rspOK
    case HttpRequest(HttpMethods.GET,"/with%20space",_,_,_) => {
      val x = ""
      sender ! rspOK
    }
		case HttpRequest(HttpMethods.GET,"/content&",_,_,_) => {
      sender ! rspBadRequest
    }
		case HttpRequest(HttpMethods.GET,"/content",_,_,_) => {
      sender ! rspNotFound
    }
		case HttpRequest(HttpMethods.GET,"/content/money",_,_,_) => {
      sender ! rspForbidden
    }
		case HttpRequest(HttpMethods.GET,"/nocontent",_,_,_) => {
      sender ! rspFailure
    }
		case x => {
      val x = ""
      sender ! Status.Failure(new IOException)
    }
	}
}

object MockHttpService
{
	val HTTP_PORT = 9588

	val index_html = <html><body><h1>Hello</h1></body></html>
	val rspOK = HttpResponse(
		status = 200, entity = HttpBody(ContentType.`text/plain`, index_html.toString())
	)
	val rspBadRequest = HttpResponse( status = 400, entity = "Bad request")
	val rspNotFound = HttpResponse( status = 404, entity = "Not found")
	val rspForbidden = HttpResponse( status = 403, entity = "Not found")
	val rspFailure = HttpResponse( status = 500, entity = "I am dead")

  def getConnectionManager:PoolingClientConnectionManager =
  {
    val schemeRegistry = new SchemeRegistry()
    schemeRegistry.register(
      new Scheme("http", HTTP_PORT, PlainSocketFactory.getSocketFactory) )

    val cm = new PoolingClientConnectionManager(schemeRegistry)
    cm.setMaxTotal(10)
    cm.setDefaultMaxPerRoute(1)
    cm
  }

  def getHttpClient(cm:ClientConnectionManager):HttpClient =
  {
    val params = new BasicHttpParams

    params.setParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, false)
    params.setParameter(CoreConnectionPNames.TCP_NODELAY, false)
    params.setParameter(CoreConnectionPNames.SO_TIMEOUT, 100)
    params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, 500)

    val client = new DefaultHttpClient(cm)
    client.setParams(params)

    client
  }
}
