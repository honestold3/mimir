package org.matruss.mimir.crawler.fixture

import akka.actor.{Status, ActorSystem, Actor}
import com.typesafe.config.ConfigFactory
import collection.mutable.Buffer
import akka.util.ConcurrentMultiMap
import java.util.{Comparator, Date}

trait MockObject

class MockRouter extends Actor with MockObject
{
	private[this] val urls = Buffer[String]("www.amazon.ca","www.google.com")
	private[this] val content = Triple("www.amazon.ca","<html><body>Hello</body></html>",1)

	def receive:Receive =
	{
		case msg: ReadInputFrom => sender ! urls
		case msg: WTarget => sender ! WContent( Some(content) )
		case x => Status.Failure(InputFileReaderException("Unknown message"))
	}
}

abstract class MockListener(val bus:ApplicationEventBus) extends Actor with MockObject
{
	type Event = bus.Event
	def classifier:bus.Classifier = ""
	val events = Buffer[Event]()

	bus.subscribe( self, classifier )

	def receive:Receive =
	{
		case event:Event => events += event
		case x => // do nothing
	}
}

object MockActorSystem
{
	def apply(name:String):ActorSystem = ActorSystem(name, ConfigFactory.load() )
}

trait MockStorage extends Storage with MockObject
{
	val cache = {
		val res = new ConcurrentMultiMap[String,Date](10, new Comparator[Date] {
			def compare (date1: Date, date2: Date): Int = date1.compareTo(date2)
		})
		res.put("www.amazon.ca", new Date)
		res.put("lvn.net", new Date)
		res
	}
}

