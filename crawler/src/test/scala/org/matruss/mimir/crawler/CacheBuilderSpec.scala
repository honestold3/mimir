package org.matruss.mimir.crawler

import akka.testkit.{TestActorRef, TestKit}
import fixture.{MockActorSystem, MockListener, MockStorage}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import java.util.Date
import collection.mutable.Set

class CacheBuilderSpec extends TestKit(MockActorSystem("CacheBuilder")) with WordSpec with BeforeAndAfterAll with EventBusMonitor
{
	override def afterAll() { system.shutdown() }

	val eventBus = new ApplicationEventBus {implicit val logger = system.log }

	"Cache builder" when {

		class MockCacheBuilder(override val bus: ApplicationEventBus) extends GenericCacheBuilder(bus) with MockStorage
		{
			val retentionTime:Long = 100000
		}
		val actor = TestActorRef ( new MockCacheBuilder( eventBus ) ).underlyingActor

		"started" should  {
			"subscribe to events" in {
				assert(actor.isSubscribed === true)
			}
		}
		"received finish reading input event" should {
			val listener1 = TestActorRef(
				new MockListener(eventBus) {override def classifier = StartCacheProcessing().classifier }
			).underlyingActor
			val listener2 = TestActorRef(
				new MockListener(eventBus) {override def classifier = FinishCacheProcessing().classifier }
			).underlyingActor

			val urls = Set[TargetUrlPair]( Pair("www.amazon.ca",1),Pair("www.google.com",1) )
			val initCache = actor.cache.keys.toBuffer
			eventBus.publish( FinishReadingInput( new Date, Pair(urls,0) ) )

			"announce (publish event) start of cache processing" in {
				val events = listener1.events
				assert(events.size === 1)
				assert(events.head.isInstanceOf[StartCacheProcessing])
			}
			"announce (publish event) end of cache processing" in {
				val events = listener2.events
				assert(events.size === 1)
				assert(events.head.isInstanceOf[FinishCacheProcessing])
			}
			"update cache" in {
				val intersect = initCache intersect urls.map(pair => pair._1).toSeq
				assert(actor.cache.keys.size === (initCache.size + urls.size - intersect.size))
				assert(actor.cache.valueIterator("www.amazon.ca").size === 2)
			}
		}
	}
}

