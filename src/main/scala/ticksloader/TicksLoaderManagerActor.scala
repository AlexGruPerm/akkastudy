package ticksloader

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.typesafe.config.{Config, ConfigFactory}

/**
  *  This is a main Actor that manage child Actors (load ticks by individual ticker_id)
  *  Created ans called by message "begin load" from Main app.
  */
class TicksLoaderManagerActor extends Actor {
  val log = Logging(context.system, this)
  val config :Config = ConfigFactory.load(s"application.conf")

  /**
    * If the gap is more then readByHours than read by this interval or all ticks.
  */
  val readByMinutes :Int = config.getInt("loader.load-property.read-by-minutes")

  val (sessSrc :CassSessionSrc.type , sessDest :CassSessionDest.type ) =
    try {
      (CassSessionSrc,CassSessionDest)
    } catch {
      case c: CassConnectException => {
        log.error("ERROR when call getPairOfConnection ex: CassConnectException ["+c.getMessage+"] Cause["+c.getCause+"]")
        throw c
      }
      case de : com.datastax.oss.driver.api.core.DriverTimeoutException =>
        log.error("ERROR when call getPairOfConnection ["+de.getMessage+"] ["+de.getCause+"] "+de.getExecutionInfo.getErrors)
        throw de
      case e: Throwable =>
        log.error("ERROR when call getPairOfConnection ["+e.getMessage+"]")
        throw e
    }

  def proccessTickers(sender :ActorRef, seqTickers :Seq[Ticker]) = {
    log.info("TicksLoaderManagerActor receive ["+seqTickers.size+"] tickers from "+sender.path.name+" first is "+seqTickers(0).tickerCode)

    //Creation Actors for each ticker and run it all.
    seqTickers.foreach{ticker =>
      log.info("Creation Actor for ["+ticker.tickerCode+"]")
      context.actorOf(IndividualTickerLoader.props(sessSrc,sessDest), "IndividualTickerLoader"+ticker.tickerId)
    }

    seqTickers.foreach{
      ticker =>
        log.info("run Actor IndividualTickerLoader"+ticker.tickerId+" for ["+ticker.tickerCode+"]")
        context.actorSelection("/user/TicksLoaderManagerActor/IndividualTickerLoader"+ticker.tickerId) !
          ("run", ticker.tickerId, ticker.tickerCode, readByMinutes)
        Thread.sleep(300)
    }
  }

  override def receive: Receive = {
    //case "stop" => context.stop(self)
    case "begin load" => context.actorOf(TickersDictActor.props(sessDest), "TickersDictActor") ! "get"
    case ("ticks_saved", tickerID :Int, tickerCode :String) => sender ! ("run", tickerID, tickerCode,readByMinutes)
    case seqTickers :Seq[Ticker] => proccessTickers(sender,seqTickers)
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object TicksLoaderManagerActor {
  def props: Props = Props(new TicksLoaderManagerActor)
}
