package ticksloader

import java.net.InetSocketAddress

import com.datastax.oss.driver.api.core.CqlSession
import com.typesafe.config.{Config, ConfigFactory}

object CassSessionFrom{
  val config :Config = ConfigFactory.load(s"application.conf")

  val nodeAddressFrom :String =  config.getString("loader.connection.address-from")
  val dcFrom :String = config.getString("loader.connection.dc-from")

  val sess :CqlSession = CqlSession.builder()
    .addContactPoint(new InetSocketAddress(nodeAddressFrom, 9042))
    .withLocalDatacenter(dcFrom).build()

  def getSess:CqlSession = sess

}