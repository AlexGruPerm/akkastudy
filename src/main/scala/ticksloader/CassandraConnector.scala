package ticksloader

import com.datastax.oss.driver.api.core.CqlSession
import java.net.InetSocketAddress

import scala.util.{Failure, Success, Try}

/**
  * val dbInst :DBImpl = new CassandraConnector(nodeAddress)
*/
class CassandraConnector(nodeAddress :String) {

  private val TrySession: Try[CqlSession] = {
    try {
      val sess = CqlSession.builder().addContactPoint(new InetSocketAddress(nodeAddress, 9042)).build()
      Success(sess)
    } catch {
      case e: Throwable => Failure(e)
    }
  }

  def getSession = TrySession

}
