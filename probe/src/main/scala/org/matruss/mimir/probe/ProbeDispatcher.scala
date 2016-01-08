package org.matruss.mimir.probe

import akka.actor.{ActorRef, Props, Actor}
import akka.routing.SmallestMailboxRouter
import akka.dispatch.{Await, Future}
import akka.util.Timeout
import akka.util.duration._
import akka.pattern.{ask,pipe}
import collection.mutable.Buffer
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.conn.scheme.{PlainSocketFactory, Scheme, SchemeRegistry}
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.client.HttpClient
import org.apache.http.params.{CoreConnectionPNames, BasicHttpParams}
import WebProbe._

/*
 This class creates pool of actors reached through the router, then sends them a piece of URLs to process.
 It blocks execution expecting probing results: it is necessary to avoid OutOfMemory problems, as memory holds many
 web pages at once each can be quite big.
 It creates Http client with its own pool connection manager for each bunch of data - it can't share it between them since
 there are so many. Sharing it will cause eventual running off available file descriptors
*/

class ProbeDispatcher extends GenericProbeDispatcher
{
  protected val router = context.actorOf(
    Props( new ContentProbe ).withRouter( SmallestMailboxRouter(ACTOR_POOL) ),
    name = "ContentProberouter"
  )
  implicit protected val timeout:Timeout = Timeout(FUTURE_TIMEOUT seconds)

  protected val socketTimeout = SOCKET_TIMEOUT
  protected val connectionTimeout = CONNECTION_TIMEOUT
  protected val connPoolMax = CONNECTION_POOL_MAX
  protected val connRouteMax = CONNECTION_ROUTE_MAX
  protected val staleConn = STALE_CONNECTION_CHECK
  protected val tcpNodelay = TCP_NODELAY
}

abstract class GenericProbeDispatcher extends Actor
{
  protected def socketTimeout:Int
  protected def connectionTimeout:Int
  protected def connPoolMax:Int
  protected def connRouteMax:Int
  protected def staleConn:Boolean
  protected def tcpNodelay:Boolean

  protected val router:ActorRef

  implicit protected val timeout:Timeout
  implicit private val dispatcher = context.dispatcher

  implicit private val logger = context.system.log
  private val defaultReply = {
    val res = Buffer[ProbeDone]()
    res += ProbeDone("OK")
    res
  }

  def receive:Receive =
  {
    case msg:SliceURL =>
    {
      logger.info("[{}] Processing slice {} of size {}", Array(this.getClass.getSimpleName, msg.index, msg.urls.size))
      val client = getHttpClient( getConnectionManager )
      try {
        val stat = Future.sequence(
          msg.urls map { url => router ? TargetURL(url, client) mapTo manifest[ProbeDone] }
        )
        val reply = Await.result(stat, timeout.duration)
        sender ! reply
      }
      finally { client.getConnectionManager.shutdown() }
    }
    case x => sender ! defaultReply
  }

  private[this] def getConnectionManager:PoolingClientConnectionManager =
  {
    val schemeRegistry = new SchemeRegistry()
    schemeRegistry.register(
      new Scheme("http", 80, PlainSocketFactory.getSocketFactory) )

    val cm = new PoolingClientConnectionManager(schemeRegistry)
    cm.setMaxTotal(CONNECTION_POOL_MAX)
    cm.setDefaultMaxPerRoute(CONNECTION_ROUTE_MAX)
    cm
  }

  private[this] def getHttpClient(cm:ClientConnectionManager):HttpClient =
  {
    val params = new BasicHttpParams

    params.setParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK,staleConn)
    params.setParameter(CoreConnectionPNames.TCP_NODELAY, tcpNodelay)
    params.setParameter(CoreConnectionPNames.SO_TIMEOUT, socketTimeout)
    params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, connectionTimeout)

    val client = new DefaultHttpClient(cm)
    client.setParams(params)

    client
  }
}
