package ticksloader

import java.time.LocalDate

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

/*
    val psFrom = cassFrom.prepare("select ddate from mts_src.ticks_count_days where ticker_id = :tickerId limit 1")
    val maxTsFrom = cassFrom.execute(psFrom.bind().setInt("tickerId", tickerID)).one().getLocalDate("ddate")
*/
    /*
    if (cassFrom.isClosed) log.error("cassFrom IS CLOSED.")
    else log.info("cassFrom IS OPENED.")
    */

    //val maxTsFrom = cassFrom.execute("select ddate from mts_src.ticks_count_days where ticker_id = 1 limit 1").one().getLocalDate("ddate")

    import com.datastax.oss.driver.api.core.cql.SimpleStatement

    import java.time.Duration
    import java.time.temporal.ChronoUnit

    val duration :java.time.Duration = Duration.of(5, ChronoUnit.SECONDS)
    /*
    val stmtMaxDate = SimpleStatement.newInstance("select ddate from mts_src.ticks_count_days where ticker_id = 1 limit 1").setTimeout(duration)
    val maxTsFrom = cassFrom.execute(stmtMaxDate).one().getLocalDate("ddate")
    val maxTsTo = cassTo.execute(stmtMaxDate).one().getLocalDate("ddate")
    log.info(" maxTsFrom = "+maxTsFrom+" maxTsTo="+maxTsTo)
    */
    val sqlMaxDdate :String = "select ddate from mts_src.ticks_count_days where ticker_id = :tickerID limit 1"
    val sqlMaxTs :String = "select max(db_tsunx) as ts from mts_src.ticks where ticker_id = :tickerID and ddate = :maxDdate allow filtering"

    val maxDdateFrom :LocalDate = cassFrom.execute(
      SimpleStatement.builder(sqlMaxDdate).setTimeout(duration)
        .addNamedValue("tickerID", tickerID)
        .build()).one().getLocalDate("ddate")

    val maxDdateTo :LocalDate = cassTo.execute(
      SimpleStatement.builder(sqlMaxDdate).setTimeout(duration)
        .addNamedValue("tickerID", tickerID)
        .build()).one().getLocalDate("ddate")

    log.info(" maxDdateFrom = " + maxDdateFrom + " maxDdateTo = " + maxDdateTo)

    val maxTsFrom :Long = cassFrom.execute(
      SimpleStatement.builder(sqlMaxTs).setTimeout(duration)
        .addNamedValue("tickerID", tickerID)
        .addNamedValue("maxDdate", maxDdateFrom)
        .build()).one().getLong("ts")

    val maxTsTo :Long = cassTo.execute(
      SimpleStatement.builder(sqlMaxTs).setTimeout(duration)
        .addNamedValue("tickerID", tickerID)
        .addNamedValue("maxDdate", maxDdateTo)
        .build()).one().getLong("ts")

    log.info(" maxTsFrom = " + maxTsFrom + " maxTsTo = " + maxTsTo)

    IndTickerLoaderState(tickerID, maxDdateFrom, maxTsFrom, maxDdateTo, maxTsTo)
  }

  override def receive: Receive = {
    case ("run", tickerID :Int, cassFrom :CqlSession, cassTo :CqlSession) => {
     log.info("Actor "+self.path.name+" running for ticker_id="+tickerID)
      val initialState :IndTickerLoaderState = getCurrentState(tickerID, cassFrom, cassTo)
      log.info("initialState="+initialState)
    }
    case "stop" => {
      log.info("Stopping "+self.path.name)
      context.stop(self)
    }
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object IndividualTickerLoader {
  def props: Props = Props(new IndividualTickerLoader)
}




