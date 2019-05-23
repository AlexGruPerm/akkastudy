package ticksloader

import akka.actor.{Actor, Props}
import akka.event.Logging

/**
  *  This is a main Actor that manage child Actors (load ticks by individual ticker_id)
  *  Created ans called by message "begin load" from Main app.
  */
class TicksLoaderManagerActor extends Actor {
  val log = Logging(context.system, this)

  override def receive: Receive = {
    case "begin load" => {
      log.info(" TicksLoaderManagerActor BEGIN LOADING TICKS.")
      val tickersDictActor = /*system*/context.actorOf(TickersDictActor.props, "TickersDictActor")
      tickersDictActor ! "get"
    }
    case "stop" => {
      log.info("Stop command from Main application. Close all.")
      context.stop(self)
    }
    //possible messages from child - tickersDictActor
    case "db_connected_successful" => log.info("Child "+sender.path.name+" respond that successfully connected to DB.")
    case "db_connection_failed"    => log.info("Child "+sender.path.name+" respond that can't connect to DB.")

    case seqTickers :Seq[Ticker] => {
      log.info("TicksLoaderManagerActor receive ["+seqTickers.size+"] tickers from "+sender.path.name)
    }

    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object TicksLoaderManagerActor {
  def props: Props = Props(new TicksLoaderManagerActor)
}
