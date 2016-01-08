package org.matruss.mimir.crawler.services

import akka.actor.Actor
import collection.mutable.Buffer

/**
 * Parses generic input file, i.e. no matter what type of input - HDFS, local fs or Avro,
 * it treats it in the same way.
 * It should not fail to prevent file monitor's future sequence not to fail too
 */
class InputFileReaderService extends Actor with EventBusMonitor
{
	private[this] val logger = context.system.log

	def receive:Receive =
	{
		// pick up file name, read it, create sequence of URLs
		// add suffix DONE to each file
		case msg:ReadInputFrom =>  {
			val urlSeq = Buffer[TargetUrlPair]()
			val src = msg.file
			val holder = src.holder
			if (holder.isDefined) {
				try {
					while(holder.hasNext) {
						val line = holder.next.split('\t')
						urlSeq += Pair(line(0).trim,line(1).trim.toInt)
					}
					sender ! urlSeq
				}
				catch {
					case e:Exception => {
						logger.error("[{}] Error while reading file {} : {}", Array(this.getClass.getSimpleName, src.name, e.toString))
						sender ! InputFileReaderService.emptyResult
					}
				}
				finally {
					holder.close()
          // todo probably don't need to rename, as we scan input only once
					//val isRenamed = src.renameTo(src.name + ".DONE")
					//if (!isRenamed) logger.warning("[{}] File {} has not been renamed", Array(this.getClass.getSimpleName, src.name))
				}
			}
			else {
				logger.error("[{}] Error while trying to open file {}", Array(this.getClass.getSimpleName, src.name))
				sender ! InputFileReaderService.emptyResult
			}
		}
		case x => {
			logger.warning("[{}] Unknown message", Array(this.getClass.getSimpleName))
			sender ! InputFileReaderService.emptyResult
		}
	}
}

object InputFileReaderService
{
	protected val emptyResult = Buffer[Pair[String, Int]]()
}

case class ReadInputFrom(file:InputSource)
