package ticksloader

import akka.actor.{Actor, ActorRef, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.{CqlSession, DefaultConsistencyLevel}
import com.typesafe.config.{Config, ConfigFactory}

/**
  *  This is a main Actor that manage child Actors (load ticks by individual ticker_id)
  *  Created ans called by message "begin load" from Main app.
  */
class TicksLoaderManagerActor extends Actor with CassQueries {
  val log = Logging(context.system, this)
  val config :Config = ConfigFactory.load(s"application.conf")

  /**
    * If the gap is more then readByHours than read by this interval or all ticks.
  */
  val readByMinutes :Int = config.getInt("loader.load-property.read-by-minutes")

  val (sessFrom :CqlSession, sessTo :CqlSession) =
    try {
      (CassSessionFrom.getSess,CassSessionTo.getSess)
    } catch {
      case c: CassConnectException => {
        log.error("ERROR when call getPairOfConnection ex: CassConnectException ["+c.getMessage+"]")
        throw c
      }
      case de : com.datastax.oss.driver.api.core.DriverTimeoutException =>
        log.error("ERROR when call getPairOfConnection ["+de.getMessage+"] ["+de.getCause+"] "+de.getExecutionInfo.getErrors)
        throw de
      case e: Throwable =>
        log.error("ERROR when call getPairOfConnection ["+e.getMessage+"]")
        throw e
    }

  val prepFirstDdateTick = sessFrom.prepare(sqlFirstDdateTick).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE).setIdempotent(true)
  val prepFirstTsFrom = sessFrom.prepare(sqlFirstTsFrom).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE).setIdempotent(true)
  val prepMaxDdateFrom = sessFrom.prepare(sqlMaxDdate).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE).setIdempotent(true)
  val prepMaxTsFrom =sessFrom.prepare(sqlMaxTs).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE).setIdempotent(true)
  val prepReadTicks = sessFrom.prepare(sqlReatTicks).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE).setIdempotent(true)

  val prepMaxDdateTo = sessTo.prepare(sqlMaxDdate).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE).setIdempotent(true)
  val prepMaxTsTo = sessTo.prepare(sqlMaxTs).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE).setIdempotent(true)
  val prepSaveTickDb = sessTo.prepare(sqlSaveTickDb).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)
  val prepSaveTicksByDay = sessTo.prepare(sqlSaveTicksByDay).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)
  val prepSaveTicksCntTotal = sessTo.prepare(sqlSaveTicksCntTotal).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)


  def proccessTickers(sender :ActorRef, seqTickers :Seq[Ticker]) = {
    log.info("TicksLoaderManagerActor receive ["+seqTickers.size+"] tickers from "+sender.path.name+" first is "+seqTickers(0).tickerCode)

    //Creation Actors for each ticker and run it all.
    seqTickers.foreach{ticker =>
      log.info("Creation Actor for ["+ticker.tickerCode+"]")
      context.actorOf(IndividualTickerLoader.props(sessFrom,sessTo), "IndividualTickerLoader"+ticker.tickerId)
    }

    seqTickers.foreach{
      ticker =>
        log.info("run Actor IndividualTickerLoader"+ticker.tickerId+" for ["+ticker.tickerCode+"]")
        context.actorSelection("/user/TicksLoaderManagerActor/IndividualTickerLoader"+ticker.tickerId) !
          ("run", ticker.tickerId, ticker.tickerCode,
            prepFirstDdateTick,
            prepFirstTsFrom,
            prepMaxDdateFrom,
            prepMaxDdateTo,
            prepMaxTsFrom,
            prepMaxTsTo,
            readByMinutes,
            prepReadTicks,
            prepSaveTickDb ,
            prepSaveTicksByDay,
            prepSaveTicksCntTotal
          )
        Thread.sleep(300)
    }
  }


  override def receive: Receive = {
    /*
    case "stop" => {
      context.stop(self)
    }
    */
    case "begin load" => context.actorOf(TickersDictActor.props(sessTo), "TickersDictActor") ! "get"
    case ("ticks_saved",tickerID :Int, tickerCode :String) =>
      log.info("TICKS_SAVED from "+sender.path.name+" FOR ["+tickerCode+"]")
      sender !
        ("run", tickerID, tickerCode,
          prepFirstDdateTick,
          prepFirstTsFrom,
          prepMaxDdateFrom,
          prepMaxDdateTo,
          prepMaxTsFrom,
          prepMaxTsTo,
          readByMinutes,
          prepReadTicks,
          prepSaveTickDb ,
          prepSaveTicksByDay,
          prepSaveTicksCntTotal
        )
    case seqTickers :Seq[Ticker] => proccessTickers(sender,seqTickers)
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object TicksLoaderManagerActor {
  def props: Props = Props(new TicksLoaderManagerActor)
}
