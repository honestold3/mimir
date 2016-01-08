package org.matruss.mimir.crawler

import akka.actor.{Props, ActorRef, Actor}
import akka.routing.SmallestMailboxRouter
import akka.pattern.ask
import java.util.Date
import akka.util.Timeout
import akka.util.duration._
import akka.dispatch.{Await, Future}
import java.lang.StringBuffer
import java.util.regex.Pattern
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.impl.conn.PoolingClientConnectionManager
import org.apache.http.conn.ClientConnectionManager
import org.apache.http.client.HttpClient
import org.apache.http.conn.scheme._
import org.apache.http.params.{BasicHttpParams, CoreConnectionPNames}

class WebHarvester(override val bus: ApplicationEventBus) extends GenericHarvester(bus)
{
	import WebHarvester._
	protected val router = context.actorOf(
		Props( new WebHarvesterService ).withRouter( SmallestMailboxRouter(WEB_HARVESTER_POOL) ),
		name = "WebHarvesterRouter"
	)
	implicit protected val timeout:Timeout = Timeout(WEB_HARVESTER_TIMEOUT seconds)

  protected val socketTimeout = SOCKET_TIMEOUT
  protected val connectionTimeout = CONNECTION_TIMEOUT
  protected val connPoolMax = CONNECTION_POOL_MAX
  protected val connRouteMax = CONNECTION_ROUTE_MAX
  protected val staleConn = STALE_CONNECTION_CHECK
  protected val tcpNodelay = TCP_NODELAY
}

/**
 * Dispatch worker actors to collect url's content for the current chunk of urls.
 * It opens a future waiting for completion of all workers before proceeding, i.e. sending collected content
 * for writing to disk.
 * It re-initializes http client for each bunch of data, to release all expired or idle http connections and prevent
 * socket leaking.
 *
 * @param bus application event bus
 */
abstract class GenericHarvester(val bus: ApplicationEventBus) extends Actor with EventBusMonitor
{
	val isSubscribed = {
		bus.subscribe(self, FinishCacheProcessing().classifier)
	}

	protected def router:ActorRef

	implicit protected val timeout:Timeout

  protected def socketTimeout:Int
  protected def connectionTimeout:Int
  protected def connPoolMax:Int
  protected def connRouteMax:Int
  protected def staleConn:Boolean
  protected def tcpNodelay:Boolean

  implicit private val dispatcher = context.dispatcher
	private val logger = context.system.log

	def receive:Receive =
	{
		case event:EventBusMonitor#FinishCacheProcessing =>
		{
      val (slice, index) = (event.payload._1, event.payload._2)
      logger.info("[{}] Start collecting URLs content for slice {} of a size {}", Array(this.getClass.getSimpleName, index, slice.size))
			bus.publish(StartWebHarvesting(new Date))
      val client = getHttpClient( getConnectionManager )
      try {
        val future = Future.sequence (
          slice map { url => router ? WTarget(url, client) mapTo manifest[WContent] }
        )
        val content = Await.result(future, timeout.duration )
        if (content.size > 0) {
          logger.info("[{}] Finished collecting URLs content for slice {}", Array(this.getClass.getSimpleName, index))
          bus.publish(FinishWebHarvesting( new Date(), Pair(content, index)) )
        }
        else {
          logger.warning("[{}] No content has been collected for slice {}", Array(this.getClass.getSimpleName, index))
          bus.publish(FinishWebHarvesting( new Date ) )
        }
      }
      catch {
        case e:Exception => {
          logger.error("[{}] Fetching web content failed because of {} : {}", Array(this.getClass.getSimpleName, e.getClass.getSimpleName, e.toString))
          bus.publish( FailedWebHarvesting (new Date, Some(e)) )
        }
      }
      finally { client.getConnectionManager.shutdown() }
		}
		case x =>   {
			logger.warning("[{}] Unknown message received {}", Array(this.getClass.getSimpleName, x.toString) )
			bus.publish( FailedWebHarvesting (new Date, Some(  WebHarvesterException("Unknown message for Web Harvester") )) )
		}
	}

	override def preRestart(reason: Throwable, message: Option[Any])
	{
		logger.error("Restaring because of error: {}", Array(reason.getMessage))
		super.preRestart(reason, message)
	}

  private[this] def getConnectionManager:PoolingClientConnectionManager =
  {
    val schemeRegistry = new SchemeRegistry()
    schemeRegistry.register(
      new Scheme("http", 80, PlainSocketFactory.getSocketFactory) )  // todo ? do we need port configurable

    val cm = new PoolingClientConnectionManager(schemeRegistry)
    cm.setMaxTotal(connPoolMax)
    cm.setDefaultMaxPerRoute(connPoolMax)
    cm
  }

  private[this] def getHttpClient(cm:ClientConnectionManager):HttpClient =
  {
    val params = new BasicHttpParams

    params.setParameter(CoreConnectionPNames.STALE_CONNECTION_CHECK, staleConn)
    params.setParameter(CoreConnectionPNames.TCP_NODELAY, tcpNodelay)
    params.setParameter(CoreConnectionPNames.SO_TIMEOUT, socketTimeout)
    params.setParameter(CoreConnectionPNames.CONNECTION_TIMEOUT, connectionTimeout)

    val client = new DefaultHttpClient(cm)
    client.setParams(params)

    client
  }
}

case class WTarget(base:EventBusMonitor#TargetUrlPair, client:HttpClient)
case class WContent(content: Option[Triple[String, String, Int]])
{
	// WARNING! It is very tempting to clean up lines for the bulk content being written to fs, not line-by-line as below
	// WARNING! However, it requires converting raw result into massive char array beforehand, which could make JVM choke due to lack of memory
	//private val p_np = Pattern.compile("[^\\p{Print}]")
  private val p_np = Pattern.compile("\\p{C}")   // that pattern should work on unicode characters as well
	private val p_ms = Pattern.compile(" +")

  def clean:WContent =
  {
    def replaceBadChars(s:String):String = p_ms.matcher(p_np.matcher(s).replaceAll(" ").trim).replaceAll(" ")
    if (content.isDefined){
      val url = replaceBadChars(content.get._1)
      val page = replaceBadChars(content.get._2)
      WContent(Some(url,page,content.get._3))
    }
    else this
  }

  def toLine:StringBuffer =
  {
    if (content.isDefined){
      val page = p_np.matcher(content.get._2).replaceAll(" ").trim
      new StringBuffer(content.get._1).append('\t')
        .append(p_ms.matcher(page).replaceAll(" ")).append('\t')
        .append(content.get._3)
    }
    else new StringBuffer
  }
}

object WebHarvester
{
	val config = ConfigurationProvider.akkaConfig
	val section = ConfigurationProvider.section
	implicit val logger = ConfigurationProvider.zoo.log

	val WEB_HARVESTER_POOL:Int = toOption { config.getInt(section + ".actors.web_harvester_pool") } getOrElse(10)
	val WEB_HARVESTER_TIMEOUT:Long = toOption { config.getLong(section + ".timeout.web_harvester") } getOrElse(1000) // seconds

  // HTTP settings
  val SOCKET_TIMEOUT = toOption { config.getInt("contexo.content-fetcher.http.socket_timeout") } getOrElse(1000)
  val CONNECTION_TIMEOUT = toOption { config.getInt("contexo.content-fetcher.http.connection_timeout") } getOrElse(5000)
  val CONNECTION_POOL_MAX = toOption { config.getInt("contexo.content-fetcher.http.connection_pool_max") } getOrElse(5000)
  val CONNECTION_ROUTE_MAX = toOption { config.getInt("contexo.content-fetcher.http.connection_route_max") } getOrElse(20)
  val STALE_CONNECTION_CHECK = toOption { config.getBoolean("contexo.content-fetcher.http.stale_connection_check") } getOrElse(false)
  val TCP_NODELAY = toOption { config.getBoolean("contexo.content-fetcher.http.tcp_nodelay") } getOrElse(false)
}

