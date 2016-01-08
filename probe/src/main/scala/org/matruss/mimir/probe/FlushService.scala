package org.matruss.mimir.probe

import akka.actor.Actor
import collection.mutable.Buffer
import org.apache.commons.io.FileUtils
import collection.JavaConversions._
import java.io.File

/*
 Writes to disk collected data for one piece of urls, appending to available file
*/
class FlushService(name:String) extends Actor
{
  private[this] val outFile = new File(name)
  private[this] var counter = 0

  implicit private val logger = context.system.log

  def receive:Receive =
  {
    case msg:Buffer[ProbeDone] =>
    {
      logger.info("[{}] Flashing to disk slice {}", Array(this.getClass.getSimpleName, counter))
      FileUtils.writeLines(outFile, msg.map(entry => entry.stat), "", true)
      counter += 1

      sender ! FlushDone("OK")
    }
    case x => sender ! FlushDone("OK")
  }
}
