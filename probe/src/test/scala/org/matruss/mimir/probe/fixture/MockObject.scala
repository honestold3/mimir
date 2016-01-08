package org.matruss.mimir.probe.fixture

import akka.actor.{Status, Actor, ActorSystem}
import com.typesafe.config.ConfigFactory
import org.matruss.mimir.probe.{ProbeDone, TargetURL}

trait MockObject

class MockRouter extends Actor with MockObject
{
  private[this] val goodStat = {
    new StringBuilder("www.amazon.ca").append(',').append("987").append(',').append("0\n").toString()
  }
  private[this] val badStat = {
    new StringBuilder("www.amazon.ca").append(',').append("0").append(',').append("-4\n").toString()
  }

  def receive:Receive =
  {
    case msg: TargetURL => sender ! ProbeDone(goodStat)
    case x => sender ! ProbeDone(badStat)
  }
}

object MockActorSystem extends MockObject
{
  def apply(name:String):ActorSystem = ActorSystem(name, ConfigFactory.load() )
}

