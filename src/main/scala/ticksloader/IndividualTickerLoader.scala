package ticksloader

import java.time.LocalDate

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement

//    import com.datastax.oss.driver.api.core.cql.SimpleStatement

class IndividualTickerLoader extends Actor {
  val log = Logging(context.system, this)


  def checkISClose(cass :CqlSession,sessType :String):Unit ={
    if (cass.isClosed){
      log.error("Session "+sessType+" is closed.")
    } /*else {
      log.info("Session "+sessType+" is opened.")
    }
    */
  }

  def getCurrentState(tickerID :Int,
                      tickerCode :String,
                      cassFrom :CqlSession,
                      cassTo :CqlSession,
                      prepMaxDdateFrom :BoundStatement,
                      prepMaxDdateTo :BoundStatement,
                      prepMaxTsFrom :BoundStatement,
                      prepMaxTsTo :BoundStatement
                     ) :IndTickerLoaderState = {
    import java.time.Duration
    import java.time.temporal.ChronoUnit

    checkISClose(cassFrom,"cassFrom")
    checkISClose(cassTo,"cassTo")

    val duration :java.time.Duration = Duration.of(15, ChronoUnit.SECONDS)

    val maxDdateTo :LocalDate = cassTo.execute(prepMaxDdateTo.setInt("tickerID",tickerID)).one().getLocalDate("ddate")
    val maxTsTo :Long = cassTo.execute(prepMaxTsTo.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateTo)).one().getLong("ts")

    val maxDdateFrom :LocalDate = cassFrom.execute(prepMaxDdateFrom.setInt("tickerID",tickerID)).one().getLocalDate("ddate")
    val maxTsFrom :Long = cassFrom.execute(prepMaxTsFrom.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateFrom)).one().getLong("ts")

    IndTickerLoaderState(tickerID, tickerCode, maxDdateFrom, maxTsFrom, maxDdateTo, maxTsTo)
  }

  override def receive: Receive = {
    case ("run",
      tickerID :Int,
      tickerCode :String,
      cassFrom :CqlSession,
      cassTo :CqlSession,
      prepMaxDdateFrom :BoundStatement,
      prepMaxDdateTo :BoundStatement,
      prepMaxTsFrom :BoundStatement,
      prepMaxTsTo :BoundStatement
      ) => {
     log.info("Actor ("+self.path.name+") running")
      val initialState :IndTickerLoaderState = getCurrentState(tickerID, tickerCode, cassFrom, cassTo,
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




