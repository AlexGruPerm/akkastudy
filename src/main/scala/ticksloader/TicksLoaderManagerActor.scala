package ticksloader

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
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


  override def receive: Receive = {
    case "begin load" => {
      log.info(" TicksLoaderManagerActor BEGIN LOADING TICKS.")
      val tickersDictActor = context.actorOf(TickersDictActor.props, "TickersDictActor")
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
      log.info("TicksLoaderManagerActor receive ["+seqTickers.size+"] tickers from "+sender.path.name+" first is "+seqTickers(0).tickerCode)
      sender ! "stop" //close child Actor "TickersDictActor"
      //Creation Actors for each ticker and run it all.
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
          c.printStackTrace
        }
        case e: Throwable => throw e
      }


      seqTickers/*.filter(elm => elm.tickerId==1)*/.foreach{
        ticker =>
          log.info("Creation new Actor for ["+ticker.tickerCode+"]")
         val thisTickerActor = context.actorOf(IndividualTickerLoader.props, "IndividualTickerLoader"+ticker.tickerId)
            thisTickerActor ! ("run", ticker.tickerId, sessFrom, sessTo)

          Thread.sleep(2000)
          /** ~~~~~~~~~~~~~~~~~~~~~~~~ */
          /*
          Thread.sleep(3000)
          thisTickerActor ! "stop"
          */
          /** ~~~~~~~~~~~~~~~~~~~~~~~~ */
      }

      sessFrom.close()
      sessTo.close()
    }

    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object TicksLoaderManagerActor {
  def props: Props = Props(new TicksLoaderManagerActor)
}
