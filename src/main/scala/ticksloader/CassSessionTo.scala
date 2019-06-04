package ticksloader

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{Config, ConfigFactory}

object CassSessionTo{
  val config :Config = ConfigFactory.load(s"application.conf")

  val nodeAddressTo :String =  config.getString("loader.connection.address-to")
  val dcTo :String = config.getString("loader.connection.dc-to")

  val sess :CqlSession = CqlSession.builder()
    .addContactPoint(new InetSocketAddress(nodeAddressTo, 9042))
    .withLocalDatacenter(dcTo).build()

  def getSess:CqlSession = sess
}
