package org.matruss.mimir.crawler

import collection.mutable.{Queue,Map}
import akka.util.ConcurrentMultiMap
import java.util.{Comparator, Date}

/**
 * Cache used to store URLs with information which should help to decide whether associated content has to be re-fetched
 * Uses Akka's concurrent multi-map - url is key, can have multiple time stamps associated with it.
 * First time stamp indicate when it was added to the cache, all other - when its content was fetched
 * Will be externalized during next development iteration
 */
object Cache
{
	// todo this object will be made external very soon
	// cach is: map [url, time added, time last harvested, to be harvested next time]
	val URL_CACHE = new ConcurrentMultiMap[String,Date](INIT_SIZE, new Comparator[Date] {
		def compare (date1: Date, date2: Date): Int = date1.compareTo(date2)
	});

	val WEB_CONTENT = Queue[Map[String, WebContent]]()

	private val INIT_SIZE = 16384;  // todo should probably be configurable
}

class WebContent;


