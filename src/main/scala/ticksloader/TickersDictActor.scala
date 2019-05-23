package ticksloader

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Try, Success, Failure}

class TickersDictActor extends Actor {

  val log = Logging(context.system, this)
  val config :Config = ConfigFactory.load(s"application.conf")

  val nodeAddress :String =  config.getString("loader.connection.address")
  log.info("TickersDictActor init nodeAddress = "+nodeAddress)

  override def receive: Receive = {
    case "get" => {
      log.info(" TickersDictActor - get tickers from dictionary .")
      log.info("...here we try connect to DB and get tickers list.")
      val sess :Try[CqlSession] = (new CassandraConnector(nodeAddress)).getSession
      sess match {
        case Success(s) => {
          log.error("Cassandra successful connection")
          context.parent ! "db_connected_successful"
          s.close()
          //Send message to parent that connected successful, and query DB.
        }
        case Failure(f) =>  {
          log.error("Cassandra get connection from ["+getClass.getName+"] error message = "+f.getMessage)
          context.parent ! "db_connection_failed"
          //Send message to parent, about fail connection.
        }
      }
      1
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






