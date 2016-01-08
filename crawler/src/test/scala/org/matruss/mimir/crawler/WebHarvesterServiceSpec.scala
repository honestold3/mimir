package org.matruss.mimir.crawler

import fixture.{MockHttpService, MockActorSystem}
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import spray.io.{SingletonHandler, IOBridge}
import akka.actor.Props
import spray.can.server.HttpServer
import akka.testkit.{ImplicitSender, TestActorRef, TestKit}
import akka.util.duration._
import spray.io.IOServer.Bound

class WebHarvesterServiceSpec extends TestKit(MockActorSystem("WebHarvesterService")) with WordSpec with BeforeAndAfterAll with ImplicitSender
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
  val actor = TestActorRef( new WebHarvesterService )

	override def afterAll() { system.shutdown() }

  "URI sanitizer" when {
    "scheme is https" should {
      "keep it in sanitized URI" in {
        val toWash = "https://amazon.ca/myaccount?a=b#c"
        val washed = WebHarvesterService.sanitize(toWash)

        assert(washed.isDefined)
        assert(washed.get.getScheme === "https")
        assert(washed.get.getHost === "amazon.ca")
        assert(washed.get.getPath === "/myaccount")
        assert(washed.get.getQuery === "a=b")
        assert(washed.get.getFragment === "c")
      }
    }
    "scheme is missing" should {
      "assign http as a scheme and return valid URI" in {
        val toWash = "amazon.ca"
        val washed = WebHarvesterService.sanitize(toWash)

        assert(washed.isDefined)
        assert(washed.get.getScheme === "http")
      }
    }
    "query string is missing" should {
      "return valid URI without query string" in {
        val toWash = "http://amazon.ca/myaccount"
        val washed = WebHarvesterService.sanitize(toWash)

        assert(washed.isDefined)
        assert(washed.get.getScheme === "http")
        assert(washed.get.getHost === "amazon.ca")
        assert(washed.get.getPath === "/myaccount")
        assert(washed.get.getQuery === null)
      }
    }
    "fragment is missing" should {
      "return a valid URI without fragment" in {
        val toWash = "http://amazon.ca/myaccount?a=b"
        val washed = WebHarvesterService.sanitize(toWash)

        assert(washed.isDefined)
        assert(washed.get.getScheme === "http")
        assert(washed.get.getHost === "amazon.ca")
        assert(washed.get.getPath === "/myaccount")
        assert(washed.get.getQuery === "a=b")
        assert(washed.get.getFragment === null)
      }
    }
    "path is missing" should {
      "return a valid URI without path" in {
        val toWash = "http://amazon.ca"
        val washed = WebHarvesterService.sanitize(toWash)

        assert(washed.isDefined)
        assert(washed.get.getScheme === "http")
        assert(washed.get.getHost === "amazon.ca")
        assert(washed.get.getPath.length === 0)
      }
    }
    "special characters in abundance" should {
      "create a valid URI" in {
        val toWash = "https://amazon.ca/my account|secret?a=[b]#c"
        val washed = WebHarvesterService.sanitize(toWash)

        assert(washed.isDefined)
      }
    }
  }

	"Web Harvester service" when {
		"Web server serving requested page is alive" when {
			"page is available (full url, no special symbols)" should {
				"return its content" in {
					val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT
					val targetRequest = WTarget(Pair(targetUrl, 1), client)
					val targetResponse = MockHttpService.index_html.toString()

					actor ! targetRequest
					val msg = expectMsgClass(5 seconds, classOf[WContent])

					Thread.sleep(100)
          assert(msg.content.isDefined)
					assert(msg.content.get._1.size === targetUrl.size)
					assert(msg.content.get._2.size === targetResponse.size)
					assert(msg.content.get._3 === 1)
				}
			}
      "page is available (special symbols in URL)" should {
        "return its content" in {
          val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/with space"
          val targetRequest = WTarget(Pair(targetUrl, 1), client)
          val targetResponse = MockHttpService.index_html.toString()

          actor ! targetRequest
          val msg = expectMsgClass(5 seconds, classOf[WContent])

          Thread.sleep(100)
          assert(msg.content.isDefined)
          assert(msg.content.get._1.size === targetUrl.size)
          assert(msg.content.get._2.size === targetResponse.size)
          assert(msg.content.get._3 === 1)
        }
      }
      "page is available (no scheme, special symbols)" should {
        "return its content" in {
          val targetUrl = "localhost:" + MockHttpService.HTTP_PORT + "/with space"
          val targetRequest = WTarget(Pair(targetUrl, 1), client)
          val targetResponse = MockHttpService.index_html.toString()

          actor ! targetRequest
          val msg = expectMsgClass(5 seconds, classOf[WContent])

          Thread.sleep(100)
          assert(msg.content.isDefined)
          assert(msg.content.get._1.size === targetUrl.size)
          assert(msg.content.get._2.size === targetResponse.size)
          assert(msg.content.get._3 === 1)
        }
      }
			"page is not available (status 404)" should {
				"fail with empty response" in {
					val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/content"
					val targetRequest = WTarget(Pair(targetUrl, 1), client)

					actor ! targetRequest

					val msg = expectMsgClass(5 seconds, classOf[WContent])
					assert(msg.content.get._2.size === 0)
				}
			}
			"bad request (status 400)" should {
				"fail with empty response" in {
					val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/content&"
					val targetRequest = WTarget(Pair(targetUrl, 1), client)

					actor ! targetRequest

					val msg = expectMsgClass(5 seconds, classOf[WContent])
					assert(msg.content.get._2.size === 0)
				}
			}
			"not authorised to get content (status 403)" should {
				"fail with empty response" in {
					val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/content/money"
					val targetRequest = WTarget(Pair(targetUrl, 1), client)

					actor ! targetRequest

					val msg = expectMsgClass(5 seconds, classOf[WContent])
					assert(msg.content.get._2.size === 0)
				}
			}
		}
		"Web server is having problems" should {
			"fail with empty response" in {
				val targetUrl = "http://localhost:" + MockHttpService.HTTP_PORT + "/nocontent"
				val targetRequest = WTarget(Pair(targetUrl, 1), client)

				actor ! targetRequest

				val msg = expectMsgClass(5 seconds, classOf[WContent])
				assert(msg.content.get._2.size === 0)
			}
		}
	}
}
