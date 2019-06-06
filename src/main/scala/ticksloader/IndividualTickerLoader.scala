package ticksloader

import java.time.LocalDate

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BatchStatement, DefaultBatchType}

//import scala.collection.JavaConverters._
//import scala.collection.JavaConverters._

class IndividualTickerLoader(cassSrc :CassSessionSrc.type, cassDest :CassSessionDest.type) extends Actor {
  val log = Logging(context.system, this)

  def checkISClose(cass :CqlSession,sessType :String):Unit ={
    if (cass.isClosed){
      log.error("Session "+sessType+" is closed.")
    }
  }


  def getCurrentState(tickerID :Int,
                      tickerCode :String) :IndTickerLoaderState = {

    val maxExistDdateDest :LocalDate = cassDest.getMaxExistDdateDest(tickerID)
    log.info("> maxExistDdateDest = ["+maxExistDdateDest+"]")

    val (maxDdateDest :LocalDate, maxTsDest :Long) = maxExistDdateDest
      match {
        case null => //todo:
          val firstSrcDate :LocalDate = cassSrc.getMinExistDdateSrc(tickerID)
          log.info("> firstSrcDate = ["+firstSrcDate+"] FOR="+tickerID)

          val firstTsSrcForDate :Long = cassSrc.getFirstTsForDateSrc(tickerID,firstSrcDate)
          log.info("> firstTsSrcForDate = ["+firstTsSrcForDate+"] for firstSrcDate=["+firstSrcDate+"] FOR="+tickerID)

        (firstSrcDate,firstTsSrcForDate)

      case maxDateDest :LocalDate => //(maxDateDest,cassDest.getMaxTsBydateDest(tickerID,maxDateDest))
        {
          val maxTsDest :Long = cassDest.getMaxTsBydateDest(tickerID,maxDateDest)
          log.info("> maxExistDdateDest IS NOT NULL. maxDateDest="+maxDateDest+" maxTsDest="+maxTsDest)
          (maxDateDest,maxTsDest)
        }

    }

    val maxDdateSrc :LocalDate = cassSrc.sess.execute(cassSrc.prepMaxDdateSrc
      .setInt("tickerID",tickerID))
      .one().getLocalDate("ddate")

    val maxTsSrc :Long = cassSrc.sess.execute(cassSrc.prepMaxTsSrc
      .setInt("tickerID",tickerID)
      .setLocalDate("maxDdate",maxDdateSrc))
      .one().getLong("ts")

    IndTickerLoaderState(tickerID, tickerCode, maxDdateSrc, maxTsSrc, maxDdateDest, maxTsDest)
  }




  def readTicksFrom(currState :IndTickerLoaderState, readByMinutes :Int) :Seq[Tick] = {
    log.info("currState.maxDdateDest="+currState.maxDdateDest)
    log.info("currState.maxTsDest="+currState.maxTsDest)
/*
    import java.util._
    val ddateBegin :LocalDate = new Date(currState.maxTsDest).toInstant().atZone(ZoneId.of("UTC")).toLocalDate()
    val ddateEnd :LocalDate = new Date((currState.maxTsDest + readByMinutes*60*1000L)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate()
*/
    log.info(">>> READ TICKS INTERVAL DDATES="+currState.maxDdateDest+" -  fromTS = "+currState.maxTsDest+" for "+currState.tickerID)//+" - "+(currState.maxTsDest + readByMinutes*60*1000L))

    val st :Seq[Tick] = Nil//cassSrc.getTicksSrc(currState.tickerID, currState.maxDdateDest, currState.maxTsDest)

    log.info("readed "+st.size+" ticks for "+currState.tickerID)

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
              builder.addStatement(cassDest.prepSaveTickDbDest
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
      cassDest.sess.execute(cassDest.prepSaveTicksByDayDest
        .setInt("tickerID", currState.tickerID)
        .setLocalDate("ddate", distDdate)
        .setLong("pTicksCount", seqReadedTicks.count(tick => tick.ddate == distDdate))
      )
    }

    cassDest.sess.execute(cassDest.prepSaveTicksCntTotalDest
      .setInt("tickerID",currState.tickerID)
      .setLong("pTicksCount",seqTicksSize)
    )

    seqTicksSize
  }

  override def receive: Receive = {
    case ("run", tickerID :Int, tickerCode :String, readByMinutes :Int)  =>
      log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      log.info("ACTOR ("+self.path.name+") IS RUNNING FOR ["+tickerCode+"]")
      val currState :IndTickerLoaderState = getCurrentState(tickerID, tickerCode)

      log.info("  FOR [" + tickerCode + "] STATE = " + currState)


      val seqReadedTicks :Seq[Tick] = readTicksFrom(currState, readByMinutes)
      log.info("    FOR [" + tickerCode + "] READ CNT = " + seqReadedTicks.size)

      /*
      val ticksSaved :Long = saveTicks(seqReadedTicks, currState)
      log.info("      FOR ["+tickerCode+"] SAVE "+ticksSaved+" TICKS.")
      */
      context.parent ! ("ticks_saved",tickerID,tickerCode)
    case "stop" =>
      log.info("Stopping "+self.path.name)
      context.stop(self)
    case _ => log.info(getClass.getName + " unknown message.")
  }

}

object IndividualTickerLoader {
  def props(cassSrc :CassSessionSrc.type, cassDest :CassSessionDest.type): Props = Props(new IndividualTickerLoader(cassSrc,cassDest))
}




