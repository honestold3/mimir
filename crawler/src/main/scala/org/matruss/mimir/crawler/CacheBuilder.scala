package org.matruss.mimir.crawler

import akka.actor.Actor
import java.util.Date
import collection.mutable.Set
import Cache._
import akka.util.ConcurrentMultiMap

trait Storage
trait DynamicStorage extends Storage { val cache:ConcurrentMultiMap[String,Date] = URL_CACHE  }

class CacheBuilder(override val bus: ApplicationEventBus) extends GenericCacheBuilder(bus) with DynamicStorage
{
	val retentionTime = CacheBuilder.CACHE_RETENTION_TIME
}

object CacheBuilder
{
	val config = ConfigurationProvider.akkaConfig
	val section = ConfigurationProvider.section
	implicit val logger = ConfigurationProvider.zoo.log

	val CACHE_RETENTION_TIME:Long = {
		val hours:Long = toOption { config.getLong(section + ".cache.retention_time") } getOrElse(24)  // hours
		java.util.concurrent.TimeUnit.MILLISECONDS.convert(hours, java.util.concurrent.TimeUnit.HOURS)
	}
}

/**
 * Builds a list of urls for content fetcher based on input received from FileMonitor, and looking up for expired entries
 * in the Cache object. This is kind of a stub implementation, since cache is re-built every time application runs
 * in the future cache will be external and preserve information between runs.
 *
 * @param bus application event bus
 */
abstract class GenericCacheBuilder(val bus: ApplicationEventBus) extends Actor with EventBusMonitor
{
	val isSubscribed = {
		bus.subscribe(self, FinishReadingInput().classifier)
	}

	def cache:ConcurrentMultiMap[String,Date]
	def retentionTime:Long
	private[this] val logger = context.system.log

	def receive:Receive =
	{
		case event:EventBusMonitor#FinishReadingInput =>
		{
      val targetUrls = Set[TargetUrlPair]()
      val (slice, index) = (event.payload._1, event.payload._2)

      logger.info("[{}] Building list of target URLs and cache update for slice {}", Array(this.getClass.getSimpleName, index))
			bus.publish(StartCacheProcessing(new Date))
			for (target <- slice)
			{
				val (url, _) = target
				val it = cache.valueIterator(url)
				if (!it.isEmpty) { if (update(url, it) ) targetUrls += target }
				else
				{
					cache.put(url, new Date)
					targetUrls += target
				}
			}
			bus.publish(FinishCacheProcessing(new Date, Pair(targetUrls,index)) )
		}
		case x => {
			logger.warning("[{}] Unknown message received " + x.toString, Array(this.getClass.getSimpleName) )
			bus.publish( FailedCacheProcessing (new Date, Some( CacheBuilderException("Unknown message for Cache Builder") )) )
		}
	}

  /**
   * Current update strategy is very simple - if url in question has been fetched more than 24 hours ago - refetch it
   *
   * @param key     cache key - url
   * @param values  all time stamps associated
   * @return        should it be fetched or not
   */
	protected def update(key:String, values:Iterator[Date]):Boolean =
	{
		val currentTime = new Date
		val currentTimeOfLife = currentTime.getTime - values.min.getTime
		if (currentTimeOfLife > retentionTime) {
			logger.debug(
				"[{}] Key {} is too old: life time {} is more than retention time {}, has to be re-fetched",
				Array(this.getClass.getSimpleName, key, currentTimeOfLife, retentionTime)
			)
			cache.remove(key)
			cache.put(key, currentTime)
			true
		}
		else {
			logger.debug("[{}] Key {} will not be re-fetched", Array(this.getClass.getSimpleName, key))
			cache.put(key, currentTime)
			false
		}
	}
}
