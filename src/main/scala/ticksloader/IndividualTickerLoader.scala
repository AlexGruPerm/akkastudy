package ticksloader

import java.time.LocalDate
import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, BoundStatement, DefaultBatchType, Row}
import scala.collection.JavaConverters._

class IndividualTickerLoader(cassFrom :CqlSession,cassTo :CqlSession) extends Actor {
  val log = Logging(context.system, this)

  def checkISClose(cass :CqlSession,sessType :String):Unit ={
    if (cass.isClosed){
      log.error("Session "+sessType+" is closed.")
    }
  }

  def getCurrentState(tickerID :Int,
                      tickerCode :String,
                      prepMaxDdateFrom :BoundStatement,
                      prepMaxDdateTo :BoundStatement,
                      prepMaxTsFrom :BoundStatement,
                      prepMaxTsTo :BoundStatement
                     ) :IndTickerLoaderState = {
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

  def readTicksFrom(currState :IndTickerLoaderState, prepReadTicks :BoundStatement, readByMinutes :Int) :Seq[Tick] = {
     cassFrom.execute(prepReadTicks
       .setInt("tickerID",currState.tickerID)
       .setLocalDate("beginDdate",currState.maxDdateTo)
       .setLong("fromTs",currState.maxTsTo)
       .setLong("toTs",currState.maxTsTo + readByMinutes*60*1000L))
       .all().iterator.asScala.toSeq.map(rowToTick)
       .toList
  }

  def saveTicks(seqReadedTicks        :Seq[Tick],
                currState             :IndTickerLoaderState,
                prepSaveTickDb        :BoundStatement,
                prepSaveTicksByDay    :BoundStatement,
                prepSaveTicksCntTotal :BoundStatement) :Long = {

    val seqTicksSize :Long = seqReadedTicks.size

    //https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/statements/batch/
    //val partSqTicks = sqTicks.grouped(5000/*65535*/)
    var batch = BatchStatement.builder(DefaultBatchType.UNLOGGED).build()
    seqReadedTicks.foreach {
      t =>
        batch.add(prepSaveTickDb
          .setInt("tickerID",t.ticker_id)
          .setLocalDate("ddate",t.ddate)
          .setLong("ts",t.ts)
          .setLong("db_tsunx",t.db_tsunx)
          .setDouble("ask",t.ask)
          .setDouble("bid",t.bid))
    }
    cassTo.execute(batch)
    batch.clear()


    cassTo.execute(prepSaveTicksByDay
      .setInt("tickerID",currState.tickerID)
      .setLocalDate("ddate",currState.maxDdateTo)
      .setLong("pTicksCount",seqTicksSize)
    )

    cassTo.execute(prepSaveTicksCntTotal
      .setInt("tickerID",currState.tickerID)
      .setLong("pTicksCount",seqTicksSize)
    )

    seqTicksSize
  }

  override def receive: Receive = {
    case ("run",
      tickerID :Int,
      tickerCode :String,
      prepMaxDdateFrom :BoundStatement,
      prepMaxDdateTo :BoundStatement,
      prepMaxTsFrom :BoundStatement,
      prepMaxTsTo :BoundStatement,
      readByMinutes :Int,
      prepReadTicks :BoundStatement,
      prepSaveTickDb        :BoundStatement,
      prepSaveTicksByDay    :BoundStatement,
      prepSaveTicksCntTotal :BoundStatement
      ) =>
     log.info("Actor ("+self.path.name+") running")
      val currState :IndTickerLoaderState = getCurrentState(tickerID, tickerCode, prepMaxDdateFrom, prepMaxDdateTo, prepMaxTsFrom, prepMaxTsTo)
      log.info("currState="+currState)
      val seqReadedTicks :Seq[Tick] = readTicksFrom(currState, prepReadTicks, readByMinutes)
      log.info(" FOR ["+currState.tickerCode+"] TICK CNT="+seqReadedTicks.size)
      val ticksSaved :Long = saveTicks(seqReadedTicks, currState, prepSaveTickDb, prepSaveTicksByDay, prepSaveTicksCntTotal)
      log.info("Saved = "+ticksSaved)
    case "stop" =>
      log.info("Stopping "+self.path.name)
      context.stop(self)
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object IndividualTickerLoader {
  def props(cassFrom :CqlSession, cassTo :CqlSession): Props = Props(new IndividualTickerLoader(cassFrom,cassTo))
}




