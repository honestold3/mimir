package org.matruss.mimir.crawler

import fixture._
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import akka.testkit.{TestKit, TestActorRef}
import akka.actor.Props
import java.util.Date
import collection.mutable.Buffer
import akka.util.duration._
import akka.util.Timeout

class InputMonitorSpec extends TestKit(MockActorSystem("FileMonitor"))
                       with WordSpec
                       with BeforeAndAfterAll
                       with EventBusMonitor
{
	override def afterAll() { system.shutdown() }

	val eventBus = new ApplicationEventBus  {implicit val logger = system.log }
	val MOCK_INPUT_PATHS = Buffer[InputSource](
		new LocalFile("content-fetcher/src/test/input") {implicit val logger = system.log},
		new LocalFile("content-fetcher/src/test/resources/application.conf") {implicit val logger = system.log}
	)

	"Input file monitor" when {

		class MockFileMonitor(override val bus: ApplicationEventBus) extends FileMonitor(bus) with MockObject
		{
			val router = system.actorOf(Props[MockRouter], name = "mockRouter1")
			val inputPaths = Buffer[InputSource]() ++ MOCK_INPUT_PATHS
      val chunkSize = 5000
			implicit val timeout:Timeout = Timeout(10L seconds)
			def convert(cls:Symbol, path:String):InputSource = new LocalFile(path) {implicit val logger = system.log}
		}

		val ref = TestActorRef ( new MockFileMonitor( eventBus ) )
		val actor = ref.underlyingActor

		"input files available and " when {
			"received event to scan input" should {
				"subscribe to events" in {
					assert(actor.isSubscribed === true)
				}

				val listener1 = TestActorRef(
					new MockListener(eventBus) {override def classifier = StartReadingInput().classifier }
				).underlyingActor
				val listener2 = TestActorRef(
					new MockListener(eventBus) {override def classifier = FinishReadingInput().classifier }
				).underlyingActor
				val listener3 = TestActorRef(
					new MockListener(eventBus) {override def classifier = FailedFileMonitor().classifier }
				).underlyingActor

				eventBus.publish( ScanInput( new Date ) )

				"announce (publish event) input processing" in {
					val events = listener1.events
					assert(events.size === 1)

					val event = events.head
					assert(event.isInstanceOf[StartReadingInput])
					events.clear()
				}
				"announce (publish event) end of input processing" in {
					val events = listener2.events
					assert(events.size === 1)

					val event = events.head
					assert(event.isInstanceOf[FinishReadingInput])

					val payload = event.asInstanceOf[FinishReadingInput].payload
					assert(payload._1.size === 2)
					events.clear()
				}
				"not fail" in {
					val events = listener3.events
					assert(events.size === 0)
				}
			}
		}
		"no input files available and" when {
			"received event to scan input" should {
				"subscribe to events" in {
					assert(actor.isSubscribed === true)
				}
				"not publish anything and not increase target url queue" in {
					val listener1 = TestActorRef(
						new MockListener(eventBus) {override def classifier = StartReadingInput().classifier }
					).underlyingActor
					val listener2 = TestActorRef(
						new MockListener(eventBus) {override def classifier = FinishReadingInput().classifier }
					).underlyingActor

					actor.inputPaths.clear()
					eventBus.publish( ScanInput( new Date ) )

					assert(listener1.events.size === 0)
					assert(listener2.events.size === 0)
				}
			}
		}
	}
}
