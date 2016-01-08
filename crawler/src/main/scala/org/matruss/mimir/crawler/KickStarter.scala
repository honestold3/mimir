package org.matruss.mimir.crawler

import akka.actor.{Status, ActorRef, Actor}

/**
 * Facilitates appropriate completion of main class's future : starts event chain by publishing ScanInput event, listens
 * to FinishDataFlushing event - indicating that all content has been written to disk, and listens for individual component's
 * failure events to complete main future prematurely with the failure.
 *
 * @param bus application event bus
 */
class KickStarter(val bus: ApplicationEventBus) extends Actor with EventBusMonitor
{
  val isSubscribed = {
    bus.subscribe(self, NoInput().classifier)
    bus.subscribe(self, FinishReadingInput().classifier)
    bus.subscribe(self, FinishDataFlushing().classifier)
    bus.subscribe(self, FailedReadingInput().classifier)
    bus.subscribe(self, FailedCacheProcessing().classifier)
    bus.subscribe(self, FailedWebHarvesting().classifier)
  }
  private val logger = context.system.log
  private[this] var origin:Option[ActorRef] = None
  private[this] var nSlices = 0
  private[this] var nFlushes = 0

  def receive:Receive =
  {
    case event:ScanInput => {
      origin = Some(sender)
      bus.publish( event )
    }
    // todo should probably check if origin is defined, but can't see how it could happen
    case event:NoInput => origin.get ! Status.Success
    case event:EventBusMonitor#FinishReadingInput => nSlices += 1
    case event:EventBusMonitor#FinishDataFlushing => {
      // todo should check a status of event, i.e. whether flushing succeeded
      nFlushes += 1
      if (nFlushes < nSlices) {
        logger.info("[{}] Flashed to disk {} out of {} data chunks", Array(this.getClass.getSimpleName, nFlushes, nSlices))
      }
      else {
        logger.info("[{}] All {} data chunks out of {} have been flashed", Array(this.getClass.getSimpleName, nFlushes, nSlices))
        origin.get ! Status.Success
      }
    }
    case event:EventBusMonitor#FailedReadingInput => origin.get ! Status.Failure(event.cause.getOrElse( ContentFetcherException("Unknown fail reading error") ) )
    case event:EventBusMonitor#FailedCacheProcessing => origin.get ! Status.Failure(event.cause.getOrElse( ContentFetcherException("Unknown fail cache processing error") ) )
    case event:EventBusMonitor#FailedWebHarvesting => origin.get ! Status.Failure(event.cause.getOrElse( ContentFetcherException("Unknown fail web harvesting error") ) )
    case x => origin.get ! Status.Failure( ContentFetcherException("Unknown error: " + x.toString) )
  }
}
