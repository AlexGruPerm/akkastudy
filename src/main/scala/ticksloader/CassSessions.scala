package ticksloader

import java.net.InetSocketAddress
import java.time.LocalDate

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.{BoundStatement, Row}
import com.typesafe.config.{Config, ConfigFactory}
import scala.collection.JavaConverters._

trait CassSession extends CassQueries {
  val config :Config = ConfigFactory.load(s"application.conf")
  val confConnectPath :String = "loader.connection."

  def getNodeAddressDc(path :String) :(String,String) =
    (config.getString(confConnectPath+"address-"+path),
      config.getString(confConnectPath+"dc-"+path))

  def createSession(node :String,dc :String,port :Int = 9042) :CqlSession =
    CqlSession.builder()
      .addContactPoint(new InetSocketAddress(node, port))
      .withLocalDatacenter(dc).build()

  def prepareSql(sess :CqlSession,sqlText :String) :BoundStatement =
    sess.prepare(sqlText).bind()

}

object CassSessionSrc extends CassSession{
  private val (node :String,dc :String) = getNodeAddressDc("src")
  val sess :CqlSession = createSession(node,dc)

  val prepFirstDdateTickSrc :BoundStatement = prepareSql(sess,sqlFirstDdateTick)
  val prepFirstTsSrc :BoundStatement = prepareSql(sess,sqlFirstTsFrom)
  val prepMaxDdateSrc :BoundStatement = prepareSql(sess,sqlMaxDdate)
  val prepMaxTsSrc :BoundStatement = prepareSql(sess,sqlMaxTs)
  val prepReadTicksSrc :BoundStatement = prepareSql(sess,sqlReadTicks)

  //todo: maybe add here local cache (with key - tickerId) to eliminate unnecessary DB queries.
  def getMinExistDdateSrc(tickerId :Int) :LocalDate =
    sess.execute(prepFirstDdateTickSrc
      .setInt("tickerID",tickerId))
      .one().getLocalDate("ddate")

  //todo: maybe add here local cache (with key - tickerId+thisDate) to eliminate unnecessary DB queries.
  def getFirstTsForDateSrc(tickerId :Int, thisDate :LocalDate) :Long =
    sess.execute(prepFirstTsSrc
      .setInt("tickerID", tickerId)
      .setLocalDate("minDdate",thisDate))
      .one().getLong("ts")

  val rowToTick :(Row => Tick) = (row: Row) =>
    Tick(
      row.getInt("ticker_id"),
      row.getLocalDate("ddate"),
      row.getLong("ts"),
      row.getLong("db_tsunx"),
      row.getDouble("ask"),
      row.getDouble("bid")
    )

  def getTicksSrc(tickerId :Int, thisDate :LocalDate, fromTs :Long) :Seq[Tick] =
    sess.execute(prepReadTicksSrc
      .setInt("tickerID",tickerId)
      .setLocalDate("readDate",thisDate)
      .setLong("fromTs",fromTs))
      .all().iterator.asScala.toSeq.map(rowToTick)
      .toList
}


object CassSessionDest extends CassSession{
  private val (node :String,dc :String) = getNodeAddressDc("dest")
  val sess :CqlSession = createSession(node,dc)

  val prepMaxDdateDest :BoundStatement = prepareSql(sess,sqlMaxDdate)
  val prepMaxTsDest :BoundStatement = prepareSql(sess,sqlMaxTs)
  val prepSaveTickDbDest :BoundStatement = prepareSql(sess,sqlSaveTickDb)
  val prepSaveTicksByDayDest :BoundStatement = prepareSql(sess,sqlSaveTicksByDay)
  val prepSaveTicksCntTotalDest :BoundStatement = prepareSql(sess,sqlSaveTicksCntTotal)

  def getMaxExistDdateDest(tickerId :Int) :LocalDate =
    sess.execute(prepMaxDdateDest
      .setInt("tickerID",tickerId))
      .one().getLocalDate("ddate")

  def getMaxTsBydateDest(tickerId :Int, thisDate :LocalDate) :Long =
    sess.execute(prepMaxTsDest
      .setInt("tickerID",tickerId)
      .setLocalDate("maxDdate",thisDate))
      .one().getLong("ts")

}