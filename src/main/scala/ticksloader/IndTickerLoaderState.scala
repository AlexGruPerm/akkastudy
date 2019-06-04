package ticksloader

import java.time.LocalDate

case class IndTickerLoaderState(tickerID     :Int,
                                tickerCode   :String,
                                maxDdateFrom :LocalDate,
                                maxTsFrom    :Long,
                                maxDdateTo   :LocalDate,
                                maxTsTo      :Long)



