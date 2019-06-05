package ticksloader

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{Config, ConfigFactory}

trait CassSession {
  val config :Config = ConfigFactory.load(s"application.conf")
  val confConnectPath :String = "loader.connection."

  def getNodeAddressDc(path :String) :(String,String) =
    (config.getString(confConnectPath+"address-"+path),
      config.getString(confConnectPath+"dc-"+path))

  def createSession(node :String,dc :String,port :Int = 9042) :CqlSession =
    CqlSession.builder()
      .addContactPoint(new InetSocketAddress(node, port))
      .withLocalDatacenter(dc).build()

}

object CassSessionFrom extends CassSession{
  val (node :String,dc :String) = getNodeAddressDc("from")
  val sess :CqlSession = createSession(node,dc)
  def getSess:CqlSession = sess
}

object CassSessionTo extends CassSession{
  val (node :String,dc :String) = getNodeAddressDc("to")
  val sess :CqlSession = createSession(node,dc)
  def getSess:CqlSession = sess
}
