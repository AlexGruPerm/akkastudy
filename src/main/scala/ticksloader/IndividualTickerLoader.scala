package ticksloader

import java.time.LocalDate

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement

//    import com.datastax.oss.driver.api.core.cql.SimpleStatement

class IndividualTickerLoader extends Actor {
  val log = Logging(context.system, this)


  def getCurrentState(tickerID :Int,
                      cassFrom :CqlSession,
                      cassTo :CqlSession,
                      prepMaxDdateFrom :BoundStatement,
                      prepMaxDdateTo :BoundStatement,
                      prepMaxTsFrom :BoundStatement,
                      prepMaxTsTo :BoundStatement
                     ) :IndTickerLoaderState = {
    import java.time.Duration
    import java.time.temporal.ChronoUnit

    val duration :java.time.Duration = Duration.of(10, ChronoUnit.SECONDS)

    val maxDdateTo :LocalDate = cassTo.execute(prepMaxDdateTo.setInt("tickerID",tickerID)).one().getLocalDate("ddate")
    val maxTsTo :Long = cassTo.execute(prepMaxTsTo.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateTo)).one().getLong("ts")

    val maxDdateFrom :LocalDate = cassFrom.execute(prepMaxDdateFrom.setInt("tickerID",tickerID)).one().getLocalDate("ddate")
    val maxTsFrom :Long = cassFrom.execute(prepMaxTsFrom.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateFrom)).one().getLong("ts")

    /*
    val sqlMaxDdate :String = "select ddate from mts_src.ticks_count_days where ticker_id = :tickerID limit 1"
    val sqlMaxTs :String = "select max(db_tsunx) as ts from mts_src.ticks where ticker_id = :tickerID and ddate = :maxDdate allow filtering"

    val maxDdateFrom :LocalDate = cassFrom.execute(
      SimpleStatement.builder(sqlMaxDdate).setTimeout(duration).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)
        .addNamedValue("tickerID", tickerID)
        .build()).one().getLocalDate("ddate")

    val maxTsFrom :Long = cassFrom.execute(
      SimpleStatement.builder(sqlMaxTs).setTimeout(duration).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)
        .addNamedValue("tickerID", tickerID)
        .addNamedValue("maxDdate", maxDdateFrom)
        .build()).one().getLong("ts")


    val maxDdateTo :LocalDate = cassTo.execute(
      SimpleStatement.builder(sqlMaxDdate).setTimeout(duration).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)
        .addNamedValue("tickerID", tickerID)
        .build()).one().getLocalDate("ddate")

    val maxTsTo :Long = cassTo.execute(
      SimpleStatement.builder(sqlMaxTs).setTimeout(duration).setConsistencyLevel(DefaultConsistencyLevel.LOCAL_ONE)
        .addNamedValue("tickerID", tickerID)
        .addNamedValue("maxDdate", maxDdateTo)
        .build()).one().getLong("ts")
*/

    IndTickerLoaderState(tickerID, maxDdateFrom, maxTsFrom, maxDdateTo, maxTsTo)
  }

  override def receive: Receive = {
    case ("run",
      tickerID :Int,
      cassFrom :CqlSession,
      cassTo :CqlSession,
      prepMaxDdateFrom :BoundStatement,
    prepMaxDdateTo :BoundStatement,
    prepMaxTsFrom :BoundStatement,
    prepMaxTsTo :BoundStatement

      ) => {
     log.info("Actor "+self.path.name+" running")
      val initialState :IndTickerLoaderState = getCurrentState(tickerID, cassFrom, cassTo,
        prepMaxDdateFrom,
        prepMaxDdateTo,
        prepMaxTsFrom,
        prepMaxTsTo)
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




