package ticksloader

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession

//    import com.datastax.oss.driver.api.core.cql.SimpleStatement


class IndividualTickerLoader extends Actor {
  val log = Logging(context.system, this)


  def getCurrentState(tickerID :Int, cassFrom :CqlSession, cassTo :CqlSession) :IndTickerLoaderState = {

    /*
     select ddate from mts_src.ticks_count_days where ticker_id = 1 limit 1;
     --2019-05-13
     select max(db_tsunx) from mts_src.ticks where ticker_id=1 and ddate='2019-05-13' allow filtering;
     --1557725014703
    */


    val psFrom = cassFrom.prepare("select ddate from mts_src.ticks_count_days where ticker_id = :tickerId limit 1")
    val maxTsFrom = cassFrom.execute(psFrom.bind().setInt("tickerId", tickerID)).one().getLocalDate("ddate")


/*
    val psTo = cassTo.prepare("select ddate from mts_src.ticks_count_days where ticker_id = :tickerId limit 1")
    val maxTsTo = cassTo.execute(psFrom.bind().setInt("tickerId", tickerID)).one().getLocalDate("ddate")
*/
    IndTickerLoaderState(tickerID,maxTsFrom,maxTsFrom/*maxTsTo*/)
  }

  override def receive: Receive = {
    case ("run", tickerID :Int, cassFrom :CqlSession, cassTo :CqlSession) => {
     log.info("Actor "+self.path.name+" running for ticker_id="+tickerID)
      val initialState :IndTickerLoaderState = getCurrentState(tickerID, cassFrom, cassTo)
      log.info("initialState="+initialState)
    }
    case "stop" => {
      log.info("Stopping "+self.path.name)
      context.stop(self)}
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object IndividualTickerLoader {
  def props: Props = Props(new IndividualTickerLoader)
}




