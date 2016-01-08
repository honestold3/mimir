package org.matruss.mimir.crawler

import akka.actor.Actor
import io.OutputSource
import java.util.Date

class ContentFlushing(override val bus:ApplicationEventBus) extends GenericContentFlashing(bus)
{
  val outputPath = ConfigurationProvider.OutputPath
}

abstract class GenericContentFlashing (val bus: ApplicationEventBus) extends Actor with EventBusMonitor
{
  val isSubscribed = {
    bus.subscribe(self, FinishWebHarvesting().classifier)
  }

  implicit private[this] val logger = context.system.log
  def outputPath:OutputSource

  def receive:Receive =
  {
    case event:EventBusMonitor#FinishWebHarvesting =>
    {
      bus.publish(StartDataFlushing(new Date))
      val (slice, index) = (event.content._1, event.content._2)
      logger.info("[{}] Flushing to disk slice {}", Array(this.getClass.getSimpleName, index))

      val status = outputPath.dump(slice)
      outputPath.flush()
      bus.publish(FinishDataFlushing(new Date))    // todo get status out
    }
    case x =>
      bus.publish(FailedDataFlushing(new Date, Some( ContentFlushingException("Unknown message has been received while flashing data"))))
  }
}

