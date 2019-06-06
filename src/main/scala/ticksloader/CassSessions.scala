package ticksloader

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.cql.BoundStatement
import com.typesafe.config.{Config, ConfigFactory}

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

  val prepFirstDdateTick = prepareSql(sess,sqlFirstDdateTick)
  val prepFirstTsFrom = prepareSql(sess,sqlFirstTsFrom)
  val prepMaxDdateFrom = prepareSql(sess,sqlMaxDdate)
  val prepMaxTsFrom = prepareSql(sess,sqlMaxTs)
  val prepReadTicks = prepareSql(sess,sqlReatTicks)
}

object CassSessionDest extends CassSession{
  private val (node :String,dc :String) = getNodeAddressDc("dest")
  val sess :CqlSession = createSession(node,dc)

  val prepMaxDdateTo = prepareSql(sess,sqlMaxDdate)
  val prepMaxTsTo = prepareSql(sess,sqlMaxTs)
  val prepSaveTickDb = prepareSql(sess,sqlSaveTickDb)
  val prepSaveTicksByDay = prepareSql(sess,sqlSaveTicksByDay)
  val prepSaveTicksCntTotal = prepareSql(sess,sqlSaveTicksCntTotal)
}
