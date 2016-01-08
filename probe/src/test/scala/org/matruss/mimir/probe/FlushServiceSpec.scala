package org.matruss.mimir.probe

import akka.testkit.{TestActorRef, ImplicitSender, TestKit}
import fixture.MockActorSystem
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, WordSpec}
import scala.collection.mutable.Buffer
import akka.util.duration._
import io.Source
import java.io.File

class FlushServiceSpec extends TestKit(MockActorSystem("FlushService")) with WordSpec with BeforeAndAfterAll with BeforeAndAfter with ImplicitSender
{
  after {
    val file = new File(path)
    file.delete()
  }
  override def afterAll() { system.shutdown() }

  val path = "src/test/input/TestProbe.out.txt"

  "Flush Service" when {
    val actor = TestActorRef( new FlushService(path) )

    val req = Buffer[ProbeDone] (
      ProbeDone(new StringBuilder("google.com").append(',').append("100").append(',').append("0\n").toString()),
      ProbeDone(new StringBuilder("slashdot.org").append(',').append("200").append(',').append("0\n").toString())
    )
    "receives sequence of Probe Done evens" should {
      "dump their content to disk and report success" in {
        actor ! req
        expectMsgClass(5 seconds, classOf[FlushDone])

        val output = Source.fromFile(path).getLines()
        assert(output.size === req.size)
      }
    }
    "receive few sequence of Probe Done events" should {
      "dump them to the disk appending to one and the same file" in {
        actor ! req
        actor ! req

        val msgs = receiveN(2, 5 seconds)
        assert(msgs.head.isInstanceOf[FlushDone])

        val output = Source.fromFile(path).getLines()
        assert(output.size === (req.size*2) )
      }
    }
  }
}
