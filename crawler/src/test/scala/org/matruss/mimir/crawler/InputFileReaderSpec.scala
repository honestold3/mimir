package org.matruss.mimir.crawler

import akka.testkit.{TestActorRef, TestKit}
import fixture.MockActorSystem
import org.scalatest.{BeforeAndAfterAll, WordSpec}
import akka.util.duration._
import akka.testkit.ImplicitSender
import collection.mutable.Buffer
import java.io.File
import scalax.file.Path
import scala.io.Source

class InputFileReaderSpec extends TestKit(MockActorSystem("FileReader")) with WordSpec with BeforeAndAfterAll with ImplicitSender
{
	val nameGood = "content-fetcher/src/test/input/part-r-00000"
	val nameBad = "src/test/input/part-r-00000000"
	val source = Source.fromFile(nameGood)

	override def afterAll()
	{
    val doneName = nameGood + ".DONE"
		val done = new File(doneName)
		if (done.exists()) {
			val content = Source.fromFile(doneName).mkString
			Path.fromString(nameGood).write(content)
			done.delete()
		}
		system.shutdown()
	}

	"Input file reader" when {
		val ref = TestActorRef ( new InputFileReaderService )
		"receiving a reference to a valid file" should {
			"generate list for target urls, add suffix to it to exclude from further processing" in {
				val good = new LocalFile(nameGood) {implicit val logger = system.log}
				val lineCount = source.getLines().size
				ref ! ReadInputFrom(good)
				val msg = expectMsgClass(5 seconds, classOf[ Buffer[String] ])
				assert(msg.size === lineCount)
			}
		}
		"receiving reference to invalid file" should {
			"return empty list" in {
				val bad = new LocalFile(nameBad) {implicit val logger = system.log}
				ref ! ReadInputFrom(bad)
				val msg = expectMsgClass(5 seconds, classOf[ Buffer[String] ])

				assert(msg.size === 0)
			}
		}
		"receiving garbage" should {
			"return empty list" in {
				ref ! "hello world"
				val msg = expectMsgClass(5 seconds, classOf[ Buffer[String] ])

				assert(msg.size === 0)
			}
		}
	}
}
