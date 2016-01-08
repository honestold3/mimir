package org.matruss.mimir.crawler

import akka.actor.{Props, ActorRef, Actor}
import akka.pattern.ask
import java.util.Date
import collection.mutable.{Buffer, Seq, Set}
import akka.routing.SmallestMailboxRouter
import akka.util.Timeout
import akka.util.duration._
import scala.Predef._
import akka.dispatch.Future
i
class InputFileMonitor(override val bus: ApplicationEventBus) extends FileMonitor(bus)
{
	import InputFileMonitor._
	val router = context.actorOf(
		Props( new InputFileReaderService ).withRouter( SmallestMailboxRouter(INPUT_READER_POOL) ),
		name = "InputProcessingRouter"
	)
	val inputPaths = INPUT_PATHS
  val chunkSize = CHUNK_SIZE
	implicit val timeout:Timeout = Timeout(INPUT_READER_TIMEOUT seconds)

	def convert(cls:Symbol, path:String):InputSource = InputSource(cls, path)
}

object InputFileMonitor
{
	val config = ConfigurationProvider.akkaConfig
	val section = ConfigurationProvider.section
	implicit val logger = ConfigurationProvider.zoo.log

	val INPUT_PATHS = ConfigurationProvider.InputPaths
	val INPUT_READER_TIMEOUT:Long = toOption { config.getLong(section + ".timeout.input_reader") } getOrElse(10) // seconds
	val INPUT_READER_POOL:Int = toOption { config.getInt(section + ".actors.input_reader_pool") } getOrElse(10)
  val CHUNK_SIZE = ConfigurationProvider.OutputChunkSize
}

/**
 * Scans input path for file, or picks up an individual file, then sends it to pool of workers for parsing
 * It's an abstract class because it's configurables picked up differently for production version and test version
 *
 * @param bus reference to event bus
 */
abstract class FileMonitor(val bus: ApplicationEventBus) extends Actor with EventBusMonitor
{
	val isSubscribed = {
		bus.subscribe(self, ScanInput().classifier)
	}

	protected def router:ActorRef
	def inputPaths:Buffer[InputSource]
	def convert(cls:Symbol, path:String):InputSource
  def chunkSize:Int

	implicit val timeout:Timeout
	implicit val dispatcher = context.dispatcher
	private[this] val logger = context.system.log

	def receive:Receive =
	{
		case event:ScanInput => scan()
		case x => {
			logger.warning("[{}] Unknown message received {}", Array(this.getClass.getSimpleName, x.toString) )
			bus.publish( FailedFileMonitor (new Date, Some( InputMonitorException("Unknown message for InputMonitor") )) )
		}
	}

	private[this] def scan()
	{
		val identitySource = {
			if(inputPaths.size > 0 ) inputPaths.head.identity
			else 'none
		}
		val namesToProcess = inputPaths flatMap  { path => path.list }
		if ( !namesToProcess.isEmpty)
		{
			bus.publish(StartReadingInput( new Date ))
			val filesToProcess =  namesToProcess map { path => convert(identitySource, path) }
			val future = Future.sequence (
				filesToProcess map { file =>
					router ? ReadInputFrom(file) mapTo manifest[Seq[TargetUrlPair]] }
			)
			future recover {
				case e: Exception => {
					logger.error("[{}] Recovering from exception caused by: {}", Array(this.getClass.getSimpleName, e.toString))
					bus.publish( FailedFileMonitor (new Date, Some(e)) )
				}
				case x => {
					logger.warning("[{}] While recovering from exception, unknown message received", Array(this.getClass.getSimpleName))
					bus.publish( FailedFileMonitor (new Date, Some( InputMonitorException("Failed recoveing from exception in file monitor") )) )
				}
			}
			future onSuccess {
				case result:Buffer[Seq[TargetUrlPair]] => {
          val targetUrls = Set[TargetUrlPair]()
          targetUrls ++= result.flatten.toSet
					if(targetUrls.size > 0) {
						val slices = dissect(targetUrls, chunkSize )
            logger.info("[{}] Input set of urls of size {} sliced to {} chunks for periodic flushing to disk", Array(this.getClass.getSimpleName, targetUrls.size, slices.size))

            slices foreach { slice => bus.publish( FinishReadingInput( new Date, slice ) ) }
						logger.info("[{}] Succesfully finished reading input files", Array(this.getClass.getSimpleName))
					}
          else {
            logger.warning("[{}] Empty result being returned by all input processing services", Array(this.getClass.getSimpleName))
            bus.publish( NoInput( new Date) )
          }
				}
				case x => {
					logger.warning("[{}] Unknown message received back from a file reader", Array(this.getClass.getSimpleName))
					bus.publish( FailedFileMonitor (new Date, Some( InputMonitorException("Unknown result being returned on success from readers in file monitor") )) )
				}
			}
			future onFailure {
				case e: Exception => {
					logger.error("[{}] Reading input files failed with the cause: {}", Array(this.getClass.getSimpleName, e.toString))
					bus.publish( FailedFileMonitor (new Date, Some(e)) )
				}
				case x => {
					logger.warning("[{}] While processing failure, unknown message has been received from a file reader", Array(this.getClass.getSimpleName))
					bus.publish( FailedFileMonitor (new Date, Some( InputMonitorException("Unknown result returned on failure from readers in file monitor") )) )
				}
			}
		}
    else
    {
      logger.info("[{}] No input to process", Array(this.getClass.getSimpleName))
      bus.publish( NoInput (new Date()) )
    }
	}

	override def preRestart(reason: Throwable, message: Option[Any])
	{
		logger.error("Restaring because of error: " +  reason.getMessage)
		super.preRestart(reason, message)
	}

  /**
   *   Split target url set on slices of pairs, where first entry is slice, and second its number(used for logging)
   */
  private def dissect(urls:Set[TargetUrlPair], sliceSize:Int):Buffer[InputSlice] =
  {
    val nSlices = urls.size/sliceSize
    val result = Buffer[Pair[Set[TargetUrlPair],Int]]()

    for (i <- 0 until nSlices ) result += Pair(urls.slice(i*sliceSize, (i+1)*sliceSize),i)
    if (urls.size%sliceSize > 0) result += Pair(urls.slice(sliceSize*nSlices, urls.size),nSlices)

    result
  }
}


