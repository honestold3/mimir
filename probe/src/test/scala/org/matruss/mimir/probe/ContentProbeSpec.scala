package org.matruss.mimir.probe

import org.matruss.mimir.probe.fixture.{MockHttpService, MockActorSystem}
import akka.actor.Props
import akka.util.duration._
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import spray.io.IOServer.Bound
import spray.io.{SingletonHandler, IOBridge}
import spray.can.server.HttpServer

class ContentProbeSpec extends TestKit(MockActorSystem("ContentProbe")) with WordSpec with BeforeAndAfterAll with ImplicitSender
{
  val ioBridge = new IOBridge(system).start()
  val handler = system.actorOf(Props(new MockHttpService))

  val server = system.actorOf(
    props = Props(new HttpServer(ioBridge, SingletonHandler(handler))),
    name = "mock-http-server"
  )
  server ! HttpServer.Bind("localhost", MockHttpService.HTTP_PORT)
  expectMsgClass(5 seconds, classOf[Bound])    // this first message is coming from server acknowledging binding

  system.registerOnTermination {
    ioBridge.stop()
  }
  val client = MockHttpService.getHttpClient( MockHttpService.getConnectionManager )

  override def afterAll() { system.shutdown() }

  "Content Probe service" when {

    val actor = TestActorRef( new ContentProbe )

    "Web server serving requested page is alive" when {
      "page is available" should {
        "return its content" in {
          val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT
          val targetRequest = TargetURL(targetUrl, client)

          actor ! targetRequest
          val msg = expectMsgClass(5 seconds, classOf[ProbeDone])

          Thread.sleep(100)
          assert(msg.stat.size > 0)

          val pieces = msg.stat.split(',')
          assert(pieces(0).size === targetUrl.size)
          assert(pieces(1).trim.toInt > 0)
          assert(pieces(2).trim.toInt < 303)
        }
      }
      "page is not available (status 404)" should {
        "fail with empty response" in {
          val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/content"
          val targetRequest = TargetURL(targetUrl, client)

          actor ! targetRequest

          val msg = expectMsgClass(5 seconds, classOf[ProbeDone])
          val pieces = msg.stat.split(',')
          assert(pieces(1).trim.toInt === 0)
          assert(pieces(2).trim.toInt === -4)
        }
      }
      "bad request (status 400)" should {
        "fail with empty response" in {
          val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/content&"
          val targetRequest = TargetURL(targetUrl, client)

          actor ! targetRequest

          val msg = expectMsgClass(5 seconds, classOf[ProbeDone])
          val pieces = msg.stat.split(',')
          assert(pieces(1).trim.toInt === 0)
          assert(pieces(2).trim.toInt === -4)
        }
      }
      "not authorised to get content (status 403)" should {
        "fail with empty response" in {
          val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/content/money"
          val targetRequest = TargetURL(targetUrl, client)

          actor ! targetRequest

          val msg = expectMsgClass(5 seconds, classOf[ProbeDone])
          val pieces = msg.stat.split(',')
          assert(pieces(1).trim.toInt === 0)
          assert(pieces(2).trim.toInt === -4)
        }
      }
    }
    "Web server is having problems" should {
      "fail with empty response" in {
        val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/nocontent"
        val targetRequest = TargetURL(targetUrl, client)

        actor ! targetRequest

        val msg = expectMsgClass(5 seconds, classOf[ProbeDone])
        val pieces = msg.stat.split(',')
        assert(pieces(1).trim.toInt === 0)
        assert(pieces(2).trim.toInt === -4)
      }
    }
  }
}
