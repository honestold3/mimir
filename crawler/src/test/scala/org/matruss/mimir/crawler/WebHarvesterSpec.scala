package org.matruss.mimir.crawler

import fixture.{MockActorSystem, MockListener, MockRouter}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import akka.actor.Props
import akka.testkit.{TestActorRef, TestKit}
import collection.mutable.Set
import java.util.Date
import akka.util.duration._
import akka.util.Timeout
import akka.event.LoggingAdapter

class WebHarvesterSpec extends TestKit(MockActorSystem("WebHarvester")) with WordSpec with BeforeAndAfterAll with EventBusMonitor
{
	override def afterAll() { system.shutdown() }

	val eventBus = new ApplicationEventBus  {implicit val logger = system.log }

	"Web harvester" when {
		class MockWebHarvester(override val bus: ApplicationEventBus) extends GenericHarvester(bus)
		{
			protected val router = system.actorOf(Props[MockRouter], name = "mockRouter")
			protected def dump(contents:Set[WContent])(implicit logger:LoggingAdapter):String = {""}
			implicit protected val timeout:Timeout = Timeout(10L seconds)

      protected val socketTimeout = 100
      protected val connectionTimeout = 500
      protected val connPoolMax = 10
      protected val connRouteMax = 1
      protected val staleConn = false
      protected val tcpNodelay = false
    }

		val actor = TestActorRef ( new MockWebHarvester( eventBus ) ).underlyingActor
		"started" should  {
			"subscribe to events" in {
				assert(actor.isSubscribed === true)
			}
		}
		"received target url set completed event" should {
			val listener1 = TestActorRef(
				new MockListener(eventBus) {override def classifier = StartWebHarvesting().classifier }
			).underlyingActor
			val listener2 = TestActorRef(
				new MockListener(eventBus) {override def classifier = FinishWebHarvesting().classifier }
			).underlyingActor
			val listener3 = TestActorRef(
				new MockListener(eventBus) {override def classifier = FailedWebHarvesting().classifier }
			).underlyingActor

			val urls = Set[TargetUrlPair]( Pair("www.amazon.ca",1),Pair("www.google.com",1) )
			eventBus.publish( FinishCacheProcessing( new Date, Pair(urls,0) ) )

			"announce (publish event) start of web harvesting" in {
				val events = listener1.events
				assert(events.size === 1)
				assert(events.head.isInstanceOf[StartWebHarvesting])
				events.clear()
			}
			"announce (publish event) end of web harvesting" in {
				val events = listener2.events
				assert(events.size === 1)
				assert(events.head.isInstanceOf[FinishWebHarvesting])
				events.clear()
			}
			"not fail" in {
				val events = listener3.events
				assert(events.size === 0)
				events.clear()
			}
			"process the same number of inputs as was published and don't fail" in {
			  eventBus.publish( FinishCacheProcessing( new Date, Pair(urls,0) ) )
				eventBus.publish( FinishCacheProcessing( new Date, Pair(urls,1) ) )
				eventBus.publish( FinishCacheProcessing( new Date, Pair(urls,3) ) )

				Thread.sleep(1000L)  // pause to let all the event get collected
				val startEvents = listener1.events
				val finishEvents = listener2.events
				val failEvents = listener3.events

				assert(startEvents.size === 3)
				assert(finishEvents.size === 3)
				assert(failEvents.size === 0)

				startEvents.clear()
				finishEvents.clear()
			}
		}
	}
}
