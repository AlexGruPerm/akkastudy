package ticksloader

import java.time.LocalDate

import akka.actor.{Actor, Props}
import akka.event.Logging
import com.datastax.oss.driver.api.core.CqlSession

//import scala.collection.JavaConverters._
//import scala.collection.JavaConverters._

class IndividualTickerLoader(cassSrc :CassSessionSrc.type, cassDest :CassSessionDest.type) extends Actor {
  val log = Logging(context.system, this)

  def checkISClose(cass :CqlSession,sessType :String):Unit ={
    if (cass.isClosed){
      log.error("Session "+sessType+" is closed.")
    }
  }


  def getCurrentState(thisTicker :Ticker) :IndTickerLoaderState = {

    val tickerID :Int = thisTicker.tickerId

    val maxExistDdateDest :LocalDate = cassDest.getMaxExistDdateDest(tickerID)
    log.info("> maxExistDdateDest = ["+maxExistDdateDest+"]")

    //todo: add check on empty source data. firstSrcDate=null firstTsSrcForDate=0L
    //todo: optimization, each load action will call this queries. cache maybe.
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //
    // Put this call into Actor "TickersDictActor" and new 2 fields in Ticker class
    //
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    val minSrcDate :LocalDate = thisTicker.minDdateSrc //cassSrc.getMinExistDdateSrc(tickerID)
    log.info("> firstSrcDate = ["+minSrcDate+"] FOR="+tickerID)
    val firstTsSrcForDate :Long = thisTicker.minTsSrc // cassSrc.getFirstTsForDateSrc(tickerID,minSrcDate)
    log.info("> firstTsSrcForDate = ["+firstTsSrcForDate+"] for firstSrcDate=["+minSrcDate+"] FOR="+tickerID)



    val (maxDdateDest :LocalDate, maxTsDest :Long) = maxExistDdateDest
      match {
        case null => (minSrcDate,firstTsSrcForDate)
      case maxDateDest :LocalDate =>
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

    IndTickerLoaderState(tickerID, thisTicker.tickerCode, minSrcDate, maxDdateSrc, maxTsSrc, maxDdateDest, maxTsDest)
  }


  def readTicksFrom(currState :IndTickerLoaderState, readByMinutes :Int) :Seq[Tick] = {
    log.info(">>> READ TICKS INTERVAL DDATES="+currState.maxDdateDest+" -  fromTS = "+currState.maxTsDest+" for "+currState.tickerID)
    val st :Seq[Tick] = cassSrc.getTicksSrc(currState.tickerID, currState.maxDdateDest, currState.maxTsDest)
    log.info("readed "+st.size+" ticks for "+currState.tickerID)
    st
    /*
    val st: Seq[Tick] =
    (currState.maxDdateSrc,currState.maxDdateDest) match {
      case (ddateSrc :LocalDate,ddateDest :LocalDate) if (ddateSrc == ddateDest) {
        log.info(">>> READ TICKS INTERVAL DDATES=" + currState.maxDdateDest + " -  fromTS = " + currState.maxTsDest + " for " + currState.tickerID)
        cassSrc.getTicksSrc(currState.tickerID, currState.maxDdateDest, currState.maxTsDest)
      }
      case _ =>{

    }
    }
    log.info("readed " + st.size + " ticks for " + currState.tickerID)
    st
    */
  }



  override def receive: Receive = {
    case ("run", thisTicker :Ticker, readByMinutes :Int)  =>
      val tickerID :Int = thisTicker.tickerId
      val tickerCode :String = thisTicker.tickerCode

      log.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
      log.info("ACTOR ("+self.path.name+") IS RUNNING FOR ["+tickerCode+"]")

      val currState :IndTickerLoaderState = getCurrentState(thisTicker)
      log.info("  FOR [" + tickerCode + "] STATE = " + currState)

      val seqReadedTicks :Seq[Tick] = readTicksFrom(currState, readByMinutes)
      log.info("    FOR [" + tickerCode + "] READ CNT = " + seqReadedTicks.size)

      val ticksSaved :Long = cassDest.saveTicks(seqReadedTicks, currState)
      log.info("      FOR ["+tickerCode+"] SAVE "+ticksSaved+" TICKS.")

      context.parent ! ("ticks_saved",thisTicker)
    case "stop" =>
      log.info("Stopping "+self.path.name)
      context.stop(self)
    case _ => log.info(getClass.getName + " unknown message.")
  }

}

object IndividualTickerLoader {
  def props(cassSrc :CassSessionSrc.type, cassDest :CassSessionDest.type): Props = Props(new IndividualTickerLoader(cassSrc,cassDest))
}




