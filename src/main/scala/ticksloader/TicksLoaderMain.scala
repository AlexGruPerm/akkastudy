package ticksloader

import akka.actor.ActorSystem

object TicksLoaderMain extends App {
  val system = ActorSystem("LoadTickersSystem")
  val ticksLoader = system.actorOf(TicksLoaderManagerActor.props, "TicksLoaderManagerActor")
  ticksLoader ! "begin load"
  ///not stoping so fast
  //ticksLoader ! "stop"
}
