package ticksloader

import java.time.LocalDate

case class IndTickerLoaderState(tickerID     :Int,
                                tickerCode   :String,
                                maxDdateFrom :LocalDate,
                                maxTsFrom    :Long,
                                maxDdateTo   :LocalDate,
                                maxTsTo      :Long){
  val gapSeconds :Long = (maxTsFrom-maxTsTo)/1000L
  val gapDays :Long = gapSeconds/60/60/24

  override def toString: String = {
    " "+tickerID+" ["+tickerCode+"] "+"("+maxDdateFrom+"-"+maxDdateTo+") ("+maxTsFrom+"-"+maxTsTo+") DAYS:"+gapDays+" SECS:"+gapSeconds
  }

}



