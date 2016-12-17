package com.argcv.valhalla.client

import java.sql.{ Connection, DriverManager, SQLException }

import com.argcv.valhalla.console.ColorForConsole._
import com.argcv.valhalla.utils.Awakable
import org.apache.commons.pool.PoolableObjectFactory
import org.apache.commons.pool.impl.GenericObjectPool

/**
 *
 * @author Yu Jing <yu@argcv.com> on 10/31/16
 */
case class MySQLClient(
  db: String = "mysql",
  auth: Option[(String, String)] = None, // user pass, authdb
  host: String = "127.0.0.1",
  port: Int = 3306,
  maxActive: Int = 10, // max alive connection
  maxWait: Long = 2000, // timeout
  whenExhaustedAction: Byte = GenericObjectPool.WHEN_EXHAUSTED_GROW) extends Awakable {
  lazy val url: String = s"jdbc:mysql://$host:$port/$db"
  val pool: GenericObjectPool[Connection] = new GenericObjectPool[Connection](new MySqlObjectFactory(), maxActive, whenExhaustedAction, maxWait)

  /**
   * @param body handle on com.udpwork.ssdb request
   * @tparam T return type
   * @return
   */
  def withClient[T](body: Connection => T): T = {
    val client: Connection = pool.borrowObject // a timeout exception may comes here
    try {
      body(client)
    } catch {
      case e: SQLException =>
        logger.warn(s"[${MySQLClient.label}] [SocketTimeoutException] ${e.getMessage} # $toString")
        throw e
      case e: Exception =>
        e.printStackTrace()
        logger.error(s"[${MySQLClient.label}] [####Exception####] ${e.getMessage} # $toString")
        throw e
    } finally {
      pool.returnObject(client)
    }
  }

  override def toString = s"[${MySQLClient.label}] $host:$port"

  def close() {
    pool.close()
  }

  private class MySqlObjectFactory() extends PoolableObjectFactory[Connection] {

    def makeObject: Connection = {
      try {
        Class.forName(MySQLClient.driverName).newInstance
        val conn: Connection = auth match {
          case Some((username, password)) =>
            DriverManager.getConnection(url, username, password)
          case None =>
            DriverManager.getConnection(url)
        }
        if (conn != null) {
          conn
        } else {
          throw new SQLException("Connection not established")
        }
      } catch {
        case e: Exception =>
          e.printStackTrace()
          throw e
      }
    }

    def destroyObject(obj: Connection) {
      obj.close()
    }

    def validateObject(obj: Connection): Boolean = {
      if (obj == null) return false
      try {
        obj.isValid(10)
      } catch {
        case e: Exception =>
          e.printStackTrace()
          false
      }
    }

    def activateObject(obj: Connection) {
      // nothing
    }

    def passivateObject(obj: Connection) {
      //obj.clearWarnings();
    }
  }

}

object MySQLClient extends Awakable {
  lazy val label = "MySQLClient".withColor(GREEN)
  //  lazy val driverName: String = "com.mysql.jdbc.Driver"
  lazy val driverName: String = "com.mysql.cj.jdbc.Driver"

}