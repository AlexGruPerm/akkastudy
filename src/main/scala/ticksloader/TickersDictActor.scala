package ticksloader

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row
import com.typesafe.config.{Config, ConfigFactory}

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class TickersDictActor extends Actor {

  val log = Logging(context.system, this)
  val config :Config = ConfigFactory.load(s"application.conf")

  val nodeAddress :String =  config.getString("loader.connection.address")
  log.info("TickersDictActor init nodeAddress = "+nodeAddress)

  val rowToTicker = (row: Row) => {
    Ticker(
      row.getInt("ticker_id"),
      row.getString("ticker_code")
     )
  }

  def readTickersFromDb(sess :CqlSession) :Seq[Ticker] = {
    import com.datastax.oss.driver.api.core.cql.SimpleStatement
    val statement = SimpleStatement.newInstance("select ticker_id,ticker_code from mts_meta.tickers")
    sess.execute(statement).all().iterator.asScala.toSeq.map(rowToTicker).sortBy(_.tickerId).toList
  }


  override def receive: Receive = {
    case "get" => {
      log.info(" TickersDictActor - get tickers from dictionary .")
      log.info("...here we try connect to DB and get tickers list.")
      val sess :Try[CqlSession] = (new CassandraConnector(nodeAddress)).getSession
      sess match {
        case Success(s) => {
          log.info("Cassandra successful connection")
          log.info("Parent name = "+context.parent.path)
          //val master = context.actorSelection("/user/TicksLoaderManagerActor")
          //master ! "db_connected_successful"
          context.parent ! "db_connected_successful"
          /**
            * Here we can query tickers dictionary and send results to parent Actor.
          */
          val distTickers :Seq[Ticker] = readTickersFromDb(s)
          context.parent ! distTickers
          s.close()
        }
        case Failure(f) =>  {
          log.error("Cassandra get connection from ["+getClass.getName+"] error message = "+f.getMessage+" - "+f.getCause)
          context.parent ! "db_connection_failed"
        }
      }
    }
    case _ => log.info(getClass.getName +" unknown message.")
  }

  override def postStop(): Unit = {
    log.info("postStop event in "+self.path.name)
  }

}

object TickersDictActor {
  def props: Props = Props(new TickersDictActor)
}






