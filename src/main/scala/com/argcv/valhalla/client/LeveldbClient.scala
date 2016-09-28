package com.argcv.valhalla.client

import java.io.File
import com.argcv.valhalla.exception.ExceptionHelper.{ SafeExec, SafeExecWithMessage, SafeExecWithTrace }

import org.iq80.leveldb.impl.{ Iq80DBFactory => LDBFactory }
import org.iq80.leveldb.{ DB => LDB, Options => LOptions }

/**
 * @param path      leveldb cache
 * @param cacheSize cache size MB in RAM
 */

case class LeveldbClient(path: String, cacheSize: Long = 0L) {

  lazy val options = new LOptions
  lazy val db: LDB = {
    options.createIfMissing(true)
    if (cacheSize > 0L)
      options.cacheSize(1048576 * cacheSize)
    // cacheSize mb
    val h = LDBFactory.factory.open(new java.io.File(path), options)
    active = true
    h
  }
  private var active = false

  def isActive: Boolean = {
    active
  }

  def close(): Boolean = {
    SafeExec {
      active = false
      db.close()
    }.isDefined
  }

  def set(k: String, v: String): Boolean = {
    SafeExec(db.put(k.asBytes, v.asBytes)).isDefined
    //    db.put(LDBFactory.bytes(k),
    //      LDBFactory.bytes(v))
  }

  def set(k: String, v: Array[Byte]): Boolean = {
    SafeExec(db.put(k.asBytes, v)).isDefined
    //    db.put(LDBFactory.bytes(k), v)
  }

  def set(k: String): Boolean = {
    SafeExec(db.put(k.asBytes, Array[Byte]())).isDefined
    //db.put(LDBFactory.bytes(k), Array[Byte]())
  }

  def getAsString(k: String): Option[String] = {
    get(k) match {
      case Some(v) => Some(v.asString)
      case None => None
    }
    //LDBFactory.asString(get(k))
  }

  def get(k: String): Option[Array[Byte]] = {
    Option(db.get(k.asBytes))
    //db.get(LDBFactory.bytes(k))
  }

  def del(k: String): Boolean = rm(k)

  def rm(k: String): Boolean = {
    SafeExec(db.delete(k.asBytes)).isDefined
    //db.delete(LDBFactory.bytes(k))
  }

  def erase(k: String) = rm(k)

  def exist(k: String) = get(k).isDefined

  def loop(handle: (String, String) => Boolean, prefix: String = ""): Unit = {
    val it = db.iterator()
    it.seek(prefix.asBytes)
    def doGet(): Boolean = {
      val kv = it.next()
      val key = kv.getKey.asString
      val value = kv.getValue.asString
      if (key.startsWith(prefix)) {
        SafeExecWithTrace(handle(key, value)) match {
          case Some(rt) => rt
          case None => false
        }
      } else {
        false
      }
    }
    while (it.hasNext && doGet) ()
  }

  /**
   * find all with the prefix of `prefix`
   *
   * @param prefix some prefix
   * @param handle handler to callback, false to stop progress
   */
  def iterWithPrefix(prefix: String = "")(handle: (String, String) => Boolean): Unit = {
    val it = db.iterator()
    it.seek(prefix.asBytes)
    def doGet(): Boolean = {
      val kv = it.next()
      val key = kv.getKey.asString
      val value = kv.getValue.asString
      if (key.startsWith(prefix)) {
        SafeExecWithTrace(handle(key, value)) match {
          case Some(rt) => rt
          case None => false
        }
      } else {
        false
      }
    }
    while (it.hasNext && doGet) ()
  }

  /**
   * start from key `start`, iter all key-value pairs, __'''never stop'''__ until return false in handler or to the end
   *
   * @param start  key to start
   * @param handle handler to callback,
   */
  def iter(start: String = "")(handle: (String, String) => Boolean): Unit = {
    val it = db.iterator()
    it.seek(start.asBytes)
    def doGet(): Boolean = {
      val kv = it.next()
      val key = kv.getKey.asString
      val value = kv.getValue.asString
      SafeExecWithTrace(handle(key, value)) match {
        case Some(rt) => rt
        case None => false
      }
    }
    while (it.hasNext && doGet) ()
  }

  implicit class StringToBytes(s: String) {
    def asBytes: Array[Byte] = LDBFactory.bytes(s)
  }

  implicit class BytesToString(a: Array[Byte]) {
    def asString: String = LDBFactory.asString(a)
  }

}

object LeveldbClient {
  def apply(path: String, cacheSize: Int): LeveldbClient =
    LeveldbClient(path, cacheSize.toLong)

  def destroy(path: String): (Boolean, String) = {
    SafeExecWithMessage(LDBFactory.factory.destroy(new File(path), new LOptions))
  }

  /**
   * WARNING: __'''UnSupported'''__
   *
   * @param path path to repair
   * @return
   */
  def repair(path: String): (Boolean, String) = {
    SafeExecWithMessage(LDBFactory.factory.repair(new File(path), new LOptions))
  }
}