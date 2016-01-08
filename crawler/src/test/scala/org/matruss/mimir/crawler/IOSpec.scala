package org.matruss.mimir.crawler

import fixture.MockActorSystem
import io.LocalOutput
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import collection.mutable.Set
import akka.testkit.{TestActorRef, ImplicitSender, TestKit}
import akka.actor.Actor
import akka.util.duration._
import scala.io.Source
import java.io.File

class IOSpec extends TestKit(MockActorSystem("MockIO")) with WordSpec with BeforeAndAfterAll with ImplicitSender
{
	override def afterAll()
	{
		system.shutdown()
	}

	val page1 = <html><body>Hello</body></html>
	val page2 = <html><body>Good bye</body></html>
	val page3 = <html><body>What do you need?</body></html>

	val good = Set[WContent]()
	val bad = Set[WContent]()

	good += (
		WContent(Some("http://google.ca", page1.toString(), 1)),
		WContent(Some("http://amazon.ca", page2.toString(), 2)),
		WContent(Some("http://kremlin.ru", page3.toString(), 3))
	)
	bad += WContent(None)

  def delete(name:String):Boolean =
  {
    val file = new File(name)
    var status = false
    if (file.exists() ) status = file.delete()
    status
  }

  "local Input" when {
    "initialized with directory" should {
      "list all files in it" in {
        val src = new LocalFile("content-fetcher/src/test/input") {implicit val logger = system.log}
        assert(src.list.size === 2)
      }
    }
    "initialized with a file" should {
      "list just this file" in {
        val src = new LocalFile("content-fetcher/src/test/input/part-r-00000") {implicit val logger = system.log}
        assert(src.list.size === 1)
      }
    }
  }

	"Local Output" when {

		class MockLocalOutput(override val base:String) extends LocalOutput(base) with Actor
		{
			implicit val logger = system.log
			def receive:Receive =
			{
				case content:Set[WContent] => sender ! dump(content)
			}
		}

    val base = "content-fetcher/src/test/input/testoutput"
    val ref = TestActorRef ( new MockLocalOutput(base) )
    val actor = ref.underlyingActor

		"receive one non-empty Web Content set" should {
			"dump it to the local file (non-empty)" in {
				ref ! good

				val status = expectMsgClass(5 seconds, classOf[ Boolean ])
        assert(status === true)

        val name = actor.name
				val dump = Source.fromFile(name)
				val lines = dump getLines() map {line =>
					val fields = line split('\t')
					WContent(Some(fields(0),fields(1),fields(2).toInt))
				} toSet

				assert(good.size === lines.size)
				assert(good === lines)

				dump.close()
				delete(name)
        Thread.sleep(100)
			}
		}

    "receive several non-empty Web Content set" should {
      "dump them all to the local file, appending to the same one" in {
        ref ! good

        val status1 = expectMsgClass(5 seconds, classOf[ Boolean ])
        assert(status1 === true)

        ref ! good

        val status2 = expectMsgClass(5 seconds, classOf[ Boolean ])
        assert(status2 === true)

        val name = actor.name
        val dump = Source.fromFile(name)
        val lines = dump getLines()

        assert(lines.size === 2*good.size)

        dump.close()
        delete(name)
        Thread.sleep(100)
      }
    }

    "receive empty Web Content set" should {
			"dump it to the local file (empty)" in {
				ref ! bad

				val status = expectMsgClass(5 seconds, classOf[ Boolean ])
        assert(status === true)

        val name = actor.name
				val dump = Source.fromFile(name)
				val lines = dump getLines() toBuffer

				assert(lines.size === 1)
				assert(lines.head.size === 0)

				dump.close()
				delete(name)
        Thread.sleep(100)
      }
		}
	}
	"Hadoop Output" should {
		"receive Web Content object and dump it to the HDFS" in (pending)
	}
	"Avro Output" should {
		"receive Web Content object and serialize it as Avro datum" in (pending)
	}
}
