package ticksloader

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.{CqlSession, DefaultConsistencyLevel}
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success, Try}

/**
  *  This is a main Actor that manage child Actors (load ticks by individual ticker_id)
  *  Created ans called by message "begin load" from Main app.
  */
class TicksLoaderManagerActor extends Actor {
  val log = Logging(context.system, this)

  /**
    * return pair of connections or erase CassConnectException
  */
  def getPairOfConnection(cassFrom :String, dcFrom :String, cassTo :String, dcTo :String) :(CqlSession,CqlSession) ={
    val sessFrom :Try[CqlSession] = new CassandraConnector(cassFrom,dcFrom).getSession
    val sessTo :Try[CqlSession] = new CassandraConnector(cassTo,dcTo).getSession

    val (sf: CqlSession, st:CqlSession) =
    (sessFrom,sessTo) match {
      case (Success(sfrom),Success(sto)) => {
        log.info("Both session opened in "+getClass.getName+" in getPairOfConnection")
        (sfrom,sto)
      }
      /**
        * Failure section.
        * */
      case (Failure(ff),Failure(ft)) => {
        log.error("Cassandra error connection ["+getClass.getName+"] errmsg (Both) = "+
          ff.getMessage+" - "+ ff.getCause+" "+ft.getMessage+" - "+ ft.getCause)
          throw CassConnectException("Cassandra source and destination :"+ff.getMessage+" "+ft.getMessage, ff.getCause)
      }
      case (Failure(ff),_) => {
        log.error("Cassandra error connection ["+getClass.getName+"] errmsg (From) = "+ff.getMessage+" - "+ff.getCause)
        throw CassConnectException("Cassandra source :"+ff.getMessage,ff.getCause)
      }
      case (_,Failure(ft)) => {
        log.error("Cassandra error connection ["+getClass.getName+"] errmsg (To) = "+ft.getMessage+" - "+ft.getCause)
        throw CassConnectException("Cassandra destination :"+ft.getMessage,ft.getCause)
      }
    }
    log.info("SUCCESSFUL CONNECTED BOTH SIDES.")
    (sf,st)
  }

  val config :Config = ConfigFactory.load(s"application.conf")
  val nodeAddressFrom :String =  config.getString("loader.connection.address-from")
  val nodeAddressTo :String =  config.getString("loader.connection.address-to")

  //select data_center from system.local
  val dcFrom :String = config.getString("loader.connection.dc-from")
  val dcTo :String = config.getString("loader.connection.dc-to")

  val (sessFrom :CqlSession, sessTo :CqlSession) =
    try {
      getPairOfConnection(nodeAddressFrom,dcFrom, nodeAddressTo,dcTo)
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

  val sqlMaxDdate :String = "select ddate from mts_src.ticks_count_days where ticker_id = :tickerID limit 1"
  val sqlMaxTs :String = "select max(db_tsunx) as ts from mts_src.ticks where ticker_id = :tickerID and ddate = :maxDdate allow filtering"

  val prepMaxDdateFrom = sessFrom.prepare(sqlMaxDdate).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)
  val prepMaxDdateTo = sessTo.prepare(sqlMaxDdate).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)

  val prepMaxTsFrom =sessFrom.prepare(sqlMaxTs).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)
  val prepMaxTsTo = sessTo.prepare(sqlMaxTs).bind().setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)

  override def receive: Receive = {
    /*
    case "stop" => {
      context.stop(self)
    }
    */
    case "begin load" => {
      log.info(" TicksLoaderManagerActor BEGIN LOADING TICKS.")
      val tickersDictActor = context.actorOf(TickersDictActor.props, "TickersDictActor")
      tickersDictActor ! ("get",sessTo)
    }
    case seqTickers :Seq[Ticker] => {
      log.info("TicksLoaderManagerActor receive ["+seqTickers.size+"] tickers from "+sender.path.name+" first is "+seqTickers(0).tickerCode)

      //Creation Actors for each ticker and run it all.
      seqTickers.foreach{ticker =>
          log.info("Creation new Actor for ["+ticker.tickerCode+"]")
          context.actorOf(IndividualTickerLoader.props, "IndividualTickerLoader"+ticker.tickerId)
      }
      Thread.sleep(1000)

      seqTickers.foreach{
        ticker =>
          log.info("run Actor IndividualTickerLoader"+ticker.tickerId+" for ["+ticker.tickerCode+"]")
          context.actorSelection("/user/TicksLoaderManagerActor/IndividualTickerLoader"+ticker.tickerId) !
            ("run", ticker.tickerId, ticker.tickerCode, sessFrom, sessTo ,
              prepMaxDdateFrom,
              prepMaxDdateTo,
              prepMaxTsFrom,
              prepMaxTsTo)
      }

      Thread.sleep(5000)
      sessFrom.close()
      sessTo.close()
    }

    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object TicksLoaderManagerActor {
  def props: Props = Props(new TicksLoaderManagerActor)
}
