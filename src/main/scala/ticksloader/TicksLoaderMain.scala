package ticksloader

import akka.actor.ActorSystem
import org.slf4j.LoggerFactory

object TicksLoaderMain extends App {
  val log = LoggerFactory.getLogger(getClass.getName)
  val system = ActorSystem("LoadTickersSystem")
  val ticksLoader = system.actorOf(TicksLoaderManagerActor.props, "TicksLoaderManagerActor")
  log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ BEGIN LOADING TICKS ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
  ticksLoader ! "begin load"
}
