package ticksloader

import java.time.LocalDate

case class IndTickerLoaderState(tickerID :Int, maxTsSrc :LocalDate, maxTsDest :LocalDate)
