package ticksloader

import java.time.LocalDate

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Row}

import scala.collection.JavaConverters._

//import scala.collection.JavaConverters._
//    import com.datastax.oss.driver.api.core.cql.SimpleStatement

class IndividualTickerLoader(cassFrom :CqlSession,cassTo :CqlSession) extends Actor {
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
                      //cassFrom :CqlSession,
                      //cassTo :CqlSession,
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

  def readTicksFrom(//cassFrom :CqlSession,
                    currState :IndTickerLoaderState, prepReadTicks :BoundStatement, readByMinutes :Int) :Seq[Tick] = {
   if ((currState.maxTsFrom-currState.maxTsTo)/1000L > readByMinutes*60 ){
     cassFrom.execute(prepReadTicks
       .setInt("tickerID",currState.tickerID)
       .setLocalDate("beginDdate",currState.maxDdateTo)
       .setLong("fromTs",currState.maxTsTo)
       .setLong("toTs",currState.maxTsTo + readByMinutes*60*1000L)).all().iterator.asScala.toSeq.map(rowToTick)
       /*.sortBy(e => (e.ticker_id,e.db_tsunx))*/.toList
   } else {
     Nil
   }
  }

  def saveTicks(//cassTo :CqlSession,
                seqReadedTicks :Seq[Tick],
                currState :IndTickerLoaderState,
                prepSaveTickDb        :BoundStatement,
                prepSaveTicksByDay    :BoundStatement,
                prepSaveTicksCntTotal :BoundStatement) = {

    seqReadedTicks.foreach {
      t =>
        cassTo.execute(prepSaveTickDb
          .setInt("tickerID",t.ticker_id)
          .setLocalDate("ddate",t.ddate)
          .setLong("ts",t.ts)
          .setLong("db_tsunx",t.db_tsunx)
          .setDouble("ask",t.ask)
          .setDouble("bid",t.bid)
        )
    }

    cassTo.execute(prepSaveTicksByDay
      .setInt("tickerID",currState.tickerID)
      .setLocalDate("ddate",currState.maxDdateTo))

    cassTo.execute(prepSaveTicksCntTotal
      .setInt("tickerID",currState.tickerID)
      .setLong("pTicksCount",seqReadedTicks.size)
    )

    log.info("SAVED "+seqReadedTicks.size+" TICKS FOR tickerID="+currState.tickerID)

  }

  override def receive: Receive = {
    case ("run",
      tickerID :Int,
      tickerCode :String,
      //cassFrom :CqlSession,
      //cassTo :CqlSession,
      prepMaxDdateFrom :BoundStatement,
      prepMaxDdateTo :BoundStatement,
      prepMaxTsFrom :BoundStatement,
      prepMaxTsTo :BoundStatement,
      readByMinutes :Int,
      prepReadTicks :BoundStatement,
      prepSaveTickDb        :BoundStatement,
      prepSaveTicksByDay    :BoundStatement,
      prepSaveTicksCntTotal :BoundStatement
      ) => {
     log.info("Actor ("+self.path.name+") running")
      val currState :IndTickerLoaderState = getCurrentState(tickerID, tickerCode, //cassFrom, cassTo,
        prepMaxDdateFrom,
        prepMaxDdateTo,
        prepMaxTsFrom,
        prepMaxTsTo)
      log.info("currState="+currState)
      val seqReadedTicks :Seq[Tick] = readTicksFrom(//cassFrom,
        currState,prepReadTicks,readByMinutes)
      log.info(" FOR ["+currState.tickerCode+"] TICK CNT="+seqReadedTicks.size)
      saveTicks(//cassTo,
        seqReadedTicks,currState,prepSaveTickDb,prepSaveTicksByDay,prepSaveTicksCntTotal)
    }
    case "stop" => {
      log.info("Stopping "+self.path.name)
      context.stop(self)
    }
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object IndividualTickerLoader {
  def props(cassFrom :CqlSession, cassTo :CqlSession): Props = Props(new IndividualTickerLoader(cassFrom,cassTo))
}




