package ticksloader

import java.time.{LocalDate, ZoneId}

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType, Row}

import scala.collection.JavaConverters._
//import scala.collection.JavaConverters._

class IndividualTickerLoader(cassSrc :CassSessionSrc.type, cassDest :CassSessionDest.type) extends Actor {
  val log = Logging(context.system, this)

  def checkISClose(cass :CqlSession,sessType :String):Unit ={
    if (cass.isClosed){
      log.error("Session "+sessType+" is closed.")
    }
  }

  def getCurrentState(tickerID :Int,
                      tickerCode :String
                     ) :IndTickerLoaderState = {

    val (maxDdateTo :LocalDate, maxTsTo :Long) =
      cassDest.sess.execute(cassDest.prepMaxDdateTo.setInt("tickerID",tickerID)).one().getLocalDate("ddate") match {
      case null => {
        val firstDdate :LocalDate = cassSrc.sess.execute(cassSrc.prepFirstDdateTick.setInt("tickerID", tickerID)).one()
          .getLocalDate("ddate")
        log.info(">>>>>>>>>> firstDdate="+firstDdate)
        val firstTs :Long = cassSrc.sess.execute(cassSrc.prepFirstTsFrom
          .setInt("tickerID", tickerID)
          .setLocalDate("minDdate",firstDdate)).one().getLong("ts")
        log.info(">>>>>>>>>> firstTs="+firstTs)
        (firstDdate,firstTs)
      }
      case ld :LocalDate =>
        (ld,cassDest.sess.execute(cassDest.prepMaxTsTo
          .setInt("tickerID",tickerID)
          .setLocalDate("maxDdate",ld)).one().getLong("ts"))
    }

    /*
    val maxTsTo :Long = maxDdateTo match {
      case null => 0L
      case _ => cassDest.sess.execute(prepMaxTsTo.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateTo)).one().getLong("ts")
    }
    */

    //val maxTsTo :Long = cassDest.sess.execute(prepMaxTsTo.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateTo)).one().getLong("ts")

    val maxDdateFrom :LocalDate = cassSrc.sess.execute(cassSrc.prepMaxDdateFrom.setInt("tickerID",tickerID)).one().getLocalDate("ddate")
    val maxTsFrom :Long = cassSrc.sess.execute(cassSrc.prepMaxTsFrom.setInt("tickerID",tickerID).setLocalDate("maxDdate",maxDdateFrom)).one().getLong("ts")


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

  def readTicksFrom(currState :IndTickerLoaderState, readByMinutes :Int) :Seq[Tick] = {
    log.info("currState.maxDdateTo="+currState.maxDdateTo)
    log.info("currState.maxTsTo="+currState.maxTsTo)

    import java.util._

    val ddateBegin :LocalDate = new Date(currState.maxTsTo).toInstant().atZone(ZoneId.of("UTC")).toLocalDate()
    val ddateEnd :LocalDate = new Date((currState.maxTsTo + readByMinutes*60*1000L)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate()

    log.info(">>> READ TICKS INTERVAL DDATES="+ddateBegin+" - "+ ddateEnd + " TS = "+currState.maxTsTo+" - "+(currState.maxTsTo + readByMinutes*60*1000L))

    val st :Seq[Tick] = cassSrc.sess.execute(cassSrc.prepReadTicks
       .setInt("tickerID",currState.tickerID)
       .setLocalDate("ddateBegin",ddateBegin)
       .setLocalDate("ddateEnd",ddateEnd)
       .setLong("fromTs",currState.maxTsTo)
       .setLong("toTs",currState.maxTsTo + readByMinutes*60*1000L))
       .all().iterator.asScala.toSeq.map(rowToTick)
       .toList


    st
  }





  def saveTicks(seqReadedTicks        :Seq[Tick],
                currState             :IndTickerLoaderState) :Long = {

    val seqTicksSize :Long = seqReadedTicks.size

    //implicit val localDateOrdering: Ordering[LocalDate] = Ordering.by(_.getDayOfYear)
    //https://docs.datastax.com/en/developer/java-driver/4.0/manual/core/statements/batch/
    //https://github.com/datastax/java-driver/blob/4.x/integration-tests/src/test/java/com/datastax/oss/driver/api/core/cql/BatchStatementIT.java

    seqReadedTicks.map(_.ddate).distinct.sortBy(_.getDayOfYear).foreach {distDdate =>
      //log.info("LOAD Parts for ddate="+distDdate)
      val partSqTicks = seqReadedTicks.filter(t => t.ddate == distDdate).grouped(5000)
      partSqTicks.foreach {
        thisPart =>
          val builder = BatchStatement.builder(DefaultBatchType.UNLOGGED)
          thisPart.foreach {
            t =>
              builder.addStatement(cassDest.prepSaveTickDb
                .setInt("tickerID", t.ticker_id)
                .setLocalDate("ddate", t.ddate)
                .setLong("ts", t.ts)
                .setLong("db_tsunx", t.db_tsunx)
                .setDouble("ask", t.ask)
                .setDouble("bid", t.bid)
              )
          }
          val batch = builder.build()
          //log.info("Batch Part statements size = " + batch.size())
          cassDest.sess.execute(batch)
          batch.clear()
      }
    }



    seqReadedTicks.map(elm => elm.ddate).distinct.foreach {distDdate =>
      //log.info("UPDATE COUNTER SaveTicksByDay for ddate="+distDdate+" PLUS_COUNT="+seqReadedTicks.count(tick => tick.ddate == distDdate))
      cassDest.sess.execute(cassDest.prepSaveTicksByDay
        .setInt("tickerID", currState.tickerID)
        .setLocalDate("ddate", distDdate)
        .setLong("pTicksCount", seqReadedTicks.count(tick => tick.ddate == distDdate))
      )
    }

    cassDest.sess.execute(cassDest.prepSaveTicksCntTotal
      .setInt("tickerID",currState.tickerID)
      .setLong("pTicksCount",seqTicksSize)
    )

    seqTicksSize
  }

  override def receive: Receive = {
    case ("run",
      tickerID :Int,
      tickerCode :String,
      readByMinutes :Int
      )  =>
     log.info("Actor ("+self.path.name+") RUNNING FOR ["+tickerCode+"]")
      val currState :IndTickerLoaderState = getCurrentState(tickerID, tickerCode)
      log.info("   FOR ["+currState.tickerCode+"] STATE="+currState)
      val seqReadedTicks :Seq[Tick] = readTicksFrom(currState, readByMinutes)
      log.info("   FOR ["+currState.tickerCode+"] READ CNT="+seqReadedTicks.size)
      val ticksSaved :Long = saveTicks(seqReadedTicks, currState)
      log.info("   FOR ["+currState.tickerCode+"] SAVED CNT="+ticksSaved)
      context.parent ! ("ticks_saved",currState.tickerID,currState.tickerCode)
    case "stop" =>
      log.info("Stopping "+self.path.name)
      context.stop(self)
    case _ => log.info(getClass.getName +" unknown message.")
  }

}

object IndividualTickerLoader {
  def props(cassSrc :CassSessionSrc.type, cassDest :CassSessionDest.type): Props = Props(new IndividualTickerLoader(cassSrc,cassDest))
}




