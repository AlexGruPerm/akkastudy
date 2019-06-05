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
                      prepFirstDdateTick :BoundStatement,
                      prepFirstTsFrom :BoundStatement,
                      prepMaxDdateFrom :BoundStatement,
                      prepMaxDdateTo :BoundStatement,
                      prepMaxTsFrom :BoundStatement,
                      prepMaxTsTo :BoundStatement
                     ) :IndTickerLoaderState = {

    val (maxDdateTo :LocalDate, maxTsTo :Long) =
      cassTo.execute(prepMaxDdateTo.setInt("tickerID",tickerID)).one().getLocalDate("ddate") match {
      case null => {
        val firstDdate :LocalDate = cassFrom.execute(prepFirstDdateTick.setInt("tickerID", tickerID)).one()
          .getLocalDate("ddate")
        log.info(">>>>>>>>>> firstDdate="+firstDdate)
        val firstTs :Long = cassFrom.execute(prepFirstTsFrom
          .setInt("tickerID", tickerID)
          .setLocalDate("minDdate",firstDdate)).one().getLong("ts")
        log.info(">>>>>>>>>> firstTs="+firstTs)
        (firstDdate,firstTs)
      }
      case ld :LocalDate =>
        (ld,cassTo.execute(prepMaxTsTo
          .setInt("tickerID",tickerID)
          .setLocalDate("maxDdate",ld)).one().getLong("ts"))
    }

    /*
    val maxTsTo :Long = maxDdateTo match {
      case null => 0L
      case _ => cassTo.execute(prepMaxTsTo.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateTo)).one().getLong("ts")
    }
    */

    //val maxTsTo :Long = cassTo.execute(prepMaxTsTo.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateTo)).one().getLong("ts")

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

    log.info("currState.maxDdateTo="+currState.maxDdateTo)
    log.info("currState.maxTsTo="+currState.maxTsTo)

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
    //https://github.com/datastax/java-driver/blob/4.x/integration-tests/src/test/java/com/datastax/oss/driver/api/core/cql/BatchStatementIT.java
    //val partSqTicks = sqTicks.grouped(5000/*65535*/)

    var builder  = BatchStatement.builder(DefaultBatchType.LOGGED)

    seqReadedTicks.foreach {
      t => builder.addStatement(prepSaveTickDb
        .setInt("tickerID",t.ticker_id)
        .setLocalDate("ddate",t.ddate)
        .setLong("ts",t.ts)
        .setLong("db_tsunx",t.db_tsunx)
        .setDouble("ask",t.ask)
        .setDouble("bid",t.bid)
      )

    }

    val batch = builder.build()
    log.info("Batch statements size = "+batch.size())
    cassTo.execute(batch)
    //batch.clear()

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
      prepFirstDdateTick :BoundStatement,
      prepFirstTsFrom :BoundStatement,
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
     log.info("Actor ("+self.path.name+") RUNNING FOR ["+tickerCode+"]")
      val currState :IndTickerLoaderState = getCurrentState(tickerID, tickerCode, prepFirstDdateTick, prepFirstTsFrom, prepMaxDdateFrom, prepMaxDdateTo, prepMaxTsFrom, prepMaxTsTo)
      log.info("   FOR ["+currState.tickerCode+"] STATE="+currState)
      val seqReadedTicks :Seq[Tick] = readTicksFrom(currState, prepReadTicks, readByMinutes)
      log.info("   FOR ["+currState.tickerCode+"] READ CNT="+seqReadedTicks.size)
      val ticksSaved :Long = saveTicks(seqReadedTicks, currState, prepSaveTickDb, prepSaveTicksByDay, prepSaveTicksCntTotal)
      log.info("   FOR ["+currState.tickerCode+"] SAVED CNT="+ticksSaved)
      context.parent ! ("ticks_saved",currState.tickerID,currState.tickerCode)
    case "stop" =>
      log.info("Stopping "+self.path.name)
      context.stop(self)
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object IndividualTickerLoader {
  def props(cassFrom :CqlSession, cassTo :CqlSession): Props = Props(new IndividualTickerLoader(cassFrom,cassTo))
}




