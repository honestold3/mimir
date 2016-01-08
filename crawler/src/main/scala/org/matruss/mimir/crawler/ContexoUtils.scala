package org.matruss.mimir.crawler

import com.typesafe.config.{Config, ConfigException}
import akka.event.LoggingAdapter

object ContexoUtils
{
	// lazy evaluation of config values to allow not to have them in config and produce nice option instead of exception
	def toOption[T](value: => T)(implicit logger:LoggingAdapter):Option[T] =
	{
		try { Some(value) }
		catch {
			case e: Exception => {
				logger.warning("[{}] While converting to option, the following error was registered: {}", Array(this.getClass.getSimpleName, e.toString) )
				None
			}
		}
	}

	def isValidConfig(config:Config, section:String):Boolean =
	{
		// todo ! add logging
		var isValid = true
		try { config.checkValid(config, section) }
		catch
			{
				case e: ConfigException => isValid = false
				case x => isValid = false
			}
		isValid
	}
}
