package org.matruss.mimir.crawler

import akka.event.{LoggingAdapter, LookupClassification, ActorEventBus}
import java.util.{Calendar, GregorianCalendar, Date}
import collection.mutable.Set

/**
 * Custom event bus used by application's constituents to exchange asynchronously with messages.
 * It defines Event class and Classifier type, current classification is based only on class name.
 * Each actor should issue a subscribe() call for each message it wants to listen.
 * Each class (not necessarily an actor) which has a reference to event bus can publish a message
 */
abstract class ApplicationEventBus extends ActorEventBus with LookupClassification
{
	type Event = BusEvent
	type Classifier = String

	implicit val logger:LoggingAdapter

  /** Should be no less than the total number of subscribers (but not necessarily equal to it) */
  protected def mapSize():Int = ApplicationEventBus.N_OF_SUBSCRIBERS
	protected def classify(event: Event): Classifier = event.classifier
	protected def publish(event:Event, subscriber:Subscriber)
	{
		logger.debug("Publishing event: {} for subscriber {}", Array(event.getClass.getSimpleName, subscriber.getClass.getSimpleName))
		subscriber ! event
	}
}

abstract class BusEvent
{
	def time:Date
	def classifier:String = this.getClass.getSimpleName
}

object ApplicationEventBus
{
	val N_OF_SUBSCRIBERS = 16
	val DEFAULT_DATE = new GregorianCalendar(1900, Calendar.DECEMBER, 25).getTime
}

trait EventBusMonitor
{
	type TargetUrlPair = Pair[String, Int]
  type InputSlice = Pair[Set[TargetUrlPair], Int]
	import ApplicationEventBus.DEFAULT_DATE

	case class StartReadingInput(time:Date = DEFAULT_DATE) extends BusEvent
	case class FinishReadingInput(time:Date = DEFAULT_DATE, payload:InputSlice = Pair(Set(),0) ) extends BusEvent

  case class FailedReadingInput(time:Date = DEFAULT_DATE, cause:Option[Throwable] = None) extends BusEvent
	case class FailedFileMonitor(time:Date = DEFAULT_DATE, cause:Option[Throwable] = None) extends BusEvent
	case class FailedCacheProcessing(time:Date = DEFAULT_DATE, cause:Option[Throwable] = None ) extends BusEvent
	case class FailedWebHarvesting(time:Date = DEFAULT_DATE, cause:Option[Throwable] = None) extends BusEvent
  case class FailedDataFlushing(time:Date = DEFAULT_DATE, cause:Option[Throwable] = None) extends BusEvent

  case class StartCacheProcessing(time:Date = DEFAULT_DATE) extends BusEvent
	case class FinishCacheProcessing(time:Date = DEFAULT_DATE, payload:InputSlice = Pair(Set(),0) ) extends BusEvent

	case class StartWebHarvesting(time:Date = DEFAULT_DATE) extends BusEvent
	case class FinishWebHarvesting(time:Date = DEFAULT_DATE, content:Pair[Set[WContent],Int] = Pair(Set(),0) ) extends BusEvent

  case class StartDataFlushing(time:Date = DEFAULT_DATE) extends BusEvent
  case class FinishDataFlushing(time:Date = DEFAULT_DATE) extends BusEvent
}

/** These events defined outside of the trait because they can be published by classes not mixing it in */
case class ScanInput(time:Date = ApplicationEventBus.DEFAULT_DATE) extends BusEvent
case class NoInput(time:Date = ApplicationEventBus.DEFAULT_DATE) extends BusEvent
