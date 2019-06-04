package ticksloader

import java.time.LocalDate

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.datastax.oss.driver.api.core.cql.Row
import scala.collection.JavaConverters._

//import scala.collection.JavaConverters._
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
    /*
    import java.time.Duration
    import java.time.temporal.ChronoUnit
    val duration :java.time.Duration = Duration.of(15, ChronoUnit.SECONDS)
    */
    /*
    checkISClose(cassFrom,"cassFrom")
    checkISClose(cassTo,"cassTo")
    */

    val maxDdateTo :LocalDate = cassTo.execute(prepMaxDdateTo.setInt("tickerID",tickerID)).one().getLocalDate("ddate")
    val maxTsTo :Long = cassTo.execute(prepMaxTsTo.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateTo)).one().getLong("ts")

    val maxDdateFrom :LocalDate = cassFrom.execute(prepMaxDdateFrom.setInt("tickerID",tickerID)).one().getLocalDate("ddate")
    val maxTsFrom :Long = cassFrom.execute(prepMaxTsFrom.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateFrom)).one().getLong("ts")

    IndTickerLoaderState(tickerID, tickerCode, maxDdateFrom, maxTsFrom, maxDdateTo, maxTsTo)
  }

  val rowToTick = (row: Row) => {
    Tick(
      row.getInt("ticker_id"),
      row.getLocalDate("ddate"),
      row.getLong("ts"),
      row.getLong("db_tsunx"),
      row.getDouble("ask"),
      row.getDouble("bid")
    )
  }

  def readTicksFrom(cassFrom :CqlSession, currState :IndTickerLoaderState, prepReadTicks :BoundStatement, readByHours :Int) :Seq[Tick] = {
   if ((currState.maxTsFrom-currState.maxTsTo)/1000L > readByHours*60*60 ){

     cassFrom.getContext

     cassFrom.execute(prepReadTicks
       .setInt("tickerID",currState.tickerID)
       .setLocalDate("beginDdate",currState.maxDdateTo)
       .setLong("fromTs",currState.maxTsTo)
       .setLong("toTs",currState.maxTsTo+readByHours*60*60*1000L)).all().iterator.asScala.toSeq.map(rowToTick).sortBy(e => (e.ticker_id,e.db_tsunx)).toList
   } else {
     Nil
   }
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
      prepMaxTsTo :BoundStatement,
      readByHours :Int,
      prepReadTicks :BoundStatement
      ) => {
     log.info("Actor ("+self.path.name+") running")
      val currState :IndTickerLoaderState = getCurrentState(tickerID, tickerCode, cassFrom, cassTo,
        prepMaxDdateFrom,
        prepMaxDdateTo,
        prepMaxTsFrom,
        prepMaxTsTo)
      log.info("currState="+currState)
      val seqReadedTicks :Seq[Tick] = readTicksFrom(cassFrom,currState,prepReadTicks,readByHours)
      log.info(" FOR ["+currState.tickerCode+"] TICK CNT="+seqReadedTicks.size)
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




