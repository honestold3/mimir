package org.matruss.mimir.probe

import akka.actor.Props
import akka.testkit.{ImplicitSender, TestKit, TestActorRef}
import fixture.{MockRouter, MockActorSystem}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import akka.util.Timeout
import akka.util.duration._
import collection.mutable.Buffer

class ProbeDispatcherSpec extends TestKit(MockActorSystem("ProbeDispatcher")) with WordSpec with BeforeAndAfterAll with ImplicitSender
{
  override def afterAll() { system.shutdown() }

  "Probe Dispatcher" when {
    class MockProbeDispatcher extends GenericProbeDispatcher
    {
      protected val router = system.actorOf(Props[MockRouter], name = "mockRouter")
      implicit protected val timeout:Timeout = Timeout(10L seconds)

      protected val socketTimeout = 100
      protected val connectionTimeout = 500
      protected val connPoolMax = 10
      protected val connRouteMax = 1
      protected val staleConn = false
      protected val tcpNodelay = false
    }

    val actor = TestActorRef ( new MockProbeDispatcher )

    "receives slice of urls to probe" should {
      "reply with collected statistics for those urls" in {
        val urls = Buffer[String]("www.amazon.com", "recepies.net", "www.apache.org")
        val slice = SliceURL(urls,1)

        actor ! slice

        val msg = expectMsgClass(5 seconds, classOf[Buffer[ProbeDone]])

        assert(msg.size === 3)
        msg foreach { res => {
            val pieces = res.stat.split(',')
            assert(pieces.size === 3)
            assert(pieces(0).trim.size > 0)
            assert(pieces(1).trim.toInt > 0)
            assert(pieces(2).trim.toInt === 0)
          }
        }
      }
    }
  }

}
