package ticksloader

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.Row

import scala.collection.JavaConverters._

class TickersDictActor(sess :CqlSession) extends Actor {

  val log = Logging(context.system, this)
  log.info("TickersDictActor init")

  val rowToTicker = (row: Row) => {
    Ticker(
      row.getInt("ticker_id"),
      row.getString("ticker_code")
     )
  }

  def readTickersFromDb/*(sess :CqlSession)*/ :Seq[Ticker] = {
    import com.datastax.oss.driver.api.core.cql.SimpleStatement
    val statement = SimpleStatement.newInstance("select ticker_id,ticker_code from mts_meta.tickers")
    sess.execute(statement).all().iterator.asScala.toSeq.map(rowToTicker).sortBy(_.tickerId).toList
  }

  override def receive: Receive = {
    case
    "get" /*("get", sess :CqlSession)*/ => {
      log.info(" TickersDictActor - get tickers from dictionary .")
      context.parent ! readTickersFromDb/*(sess)*/.filter(t => Seq(1).contains(t.tickerId))
    }
    case "stop" => context.stop(self)
    case _ => log.info(getClass.getName +" unknown message.")
  }

  override def postStop(): Unit = {
    log.info("postStop event in "+self.path.name)
  }

}

object TickersDictActor {
  def props(sess :CqlSession): Props = Props(new TickersDictActor(sess))
}






