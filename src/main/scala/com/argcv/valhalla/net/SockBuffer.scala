package com.argcv.valhalla.net

import java.net.Socket

import com.argcv.valhalla.exception.ExceptionHelper.SafeExecWithTrace
import com.argcv.valhalla.net.DataSerializer.IntSerializer

import scala.tools.nsc.interpreter.OutputStream

/**
 * @param host    host "127.0.0.1" etc.
 * @param port    port "80" etc.
 * @param timeout timeout in ms, 0 for never timeout
 */

case class SockBuffer(host: String, port: Int, timeout: Int = 0) {
  var sock: Socket = init()

  /**
   * @return status: is closed
   */
  def isClosed: Boolean =
    sock.isClosed

  /**
   * @return status: is connected
   */
  def isConnected: Boolean =
    sock.isConnected

  /**
   * @param buff string to sent
   * @return is successfully sent
   */
  def send(buff: String): Boolean = {
    val bytes: Array[Byte] = buff.getBytes
    send(bytes, 0, bytes.length)
  }

  /**
   * @param buff string to sent
   * @param off  offset
   * @param len  size to sent
   * @return is successfully sent
   */
  def send(buff: Array[Byte], off: Int, len: Int): Boolean = {
    try {
      val os: OutputStream = sock.getOutputStream
      os.write(buff, off, len)
      os.flush()
      true
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        activate() // reconnect
        false
    }
  }

  /**
   * send a length first, and then send
   *
   * @param buff buffer to sent
   * @return
   */
  def sendWithLength(buff: String): Boolean = {
    val bytes: Array[Byte] = buff.getBytes
    val lenArray = bytes.length.getByteArray
    send(lenArray, 0, lenArray.length) && send(bytes, 0, bytes.length)
  }

  /**
   * Reads up to <code>len</code> bytes of data from the input stream into
   * an array of bytes.  An attempt is made to read as many as
   * <code>len</code> bytes, but a smaller number may be read.
   *
   * the result will converted to string
   *
   * @param len the maximum number of bytes to read.
   * @return
   */
  def recvAsString(len: Int): Option[String] = {
    recv(len) match {
      case Some(s) => Some(new String(s))
      case None => None
    }
  }

  /**
   * Reads up to <code>len</code> bytes of data from the input stream into
   * an array of bytes.  An attempt is made to read as many as
   * <code>len</code> bytes, but a smaller number may be read.
   *
   * @param len the maximum number of bytes to read.
   * @return
   */
  def recv(len: Int): Option[Array[Byte]] = {
    SafeExecWithTrace {
      val is = sock.getInputStream
      val rt = new Array[Byte](len)
      val rl = is.read(rt, 0, len)
      Some(rt.take(rl))
    } match {
      case Some(resp) =>
        resp
      case None =>
        //some exception occured, try to reconnect
        activate()
        None
    }
  }

  /**
   * reactivate
   *
   * @return is success?
   */
  def activate(): Boolean = {
    try {
      close()
      sock = init()
      true
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        false
    }
  }

  /**
   * @return socket with timeout
   */
  private def init() = {
    val so = new Socket(host, port)
    if (timeout > 0)
      so.setSoTimeout(timeout)
    so.setTcpNoDelay(true)
    so
  }

  /**
   * Closes this socket.
   * <p>
   * Any thread currently blocked in an I/O operation upon this socket
   * will return false
   * <p>
   * Once a socket has been closed, it is not available for further networking
   * use (i.e. can't be reconnected or rebound). A new socket needs to be
   * created.
   *
   * <p> Closing this socket will also close the socket's InputStream and OutputStream
   *
   * <p> If this socket has an associated channel then the channel is closed
   * as well.
   *
   * @return execute result
   */
  def close(): Boolean = {
    try {
      sock.close()
      true
    } catch {
      case t: Throwable =>
        t.printStackTrace()
        false
    }
  }

}