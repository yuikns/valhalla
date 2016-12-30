package com.argcv.valhalla.client

import java.net.{ ConnectException, SocketTimeoutException }

import com.argcv.valhalla.exception.ExceptionHelper._
import com.argcv.valhalla.string.StringHelper._
import com.argcv.valhalla.utils.Awakable
import com.argcv.valhalla.utils.CommonHelper._
import com.udpwork.ssdb.{ Response, SSDB }
import org.apache.commons.pool.PoolableObjectFactory
import org.apache.commons.pool.impl.GenericObjectPool
import org.apache.commons.pool.impl.GenericObjectPool._

import scala.collection.mutable.{ ArrayBuffer, ListBuffer }
import scala.concurrent._

/**
 * SSDB is a key-value databases base on leveldb which wrapped a tcp based api.
 * You can find document from : http://ssdb.io/docs/php/index.html or http://ssdb.io/docs/zh_cn/php/index.html
 *
 * NOTICE: please check the limitation before set&amp;get key and value <br/>
 *
 *
 * http://ssdb.io/docs/limit.html
 */
trait SSDBClient extends Awakable {

  def ttl(key: String, pool: SSDBPool): Option[Long] =
    pool.safeWithClient(_.ttl(key)) match {
      case Some(v) =>
        if (v == null || v == -1L) {
          None
        } else {
          Some(v.toLong)
        }
      case None =>
        None
    }

  /**
   * Set the value of the key. with expire
   * this function impliment it by client
   *
   * @param key   key
   * @param value value
   * @param ttl   time expired
   * @param pool  pool
   * @return
   */
  def setx(key: String, value: String, ttl: Long, pool: SSDBPool): Boolean =
    set(key, value, pool) && expire(key, ttl, pool)

  /**
   * set value by key.
   * set ttl if key does not exists
   * this function impliment it by client
   *
   * @param key   key
   * @param value value
   * @param ttl   ttl (by seconds)
   * @param pool  pool
   * @return
   */
  def setnx(key: String, value: String, ttl: Long, pool: SSDBPool): Boolean =
    get(key, pool) match {
      case Some(_) =>
        set(key, value, pool)
      case None =>
        set(key, value, pool)
        expire(key, ttl, pool)
    }

  /**
   * Set the time left to live in seconds, only for keys of KV type.
   */
  def expire(key: String, ttl: Long, pool: SSDBPool): Boolean =
    pool.execWithClient(_.expire(key, ttl))._1

  /**
   * Set the value of the key.
   *
   * @param key   key, limit: length should less than 200
   * @param value value limit: length should less than 30MB
   * @param pool  pool
   */
  def set(key: String, value: String, pool: SSDBPool): Boolean =
    pool.execWithClient(_.set(key, value))._1

  //  /**
  //   * Set the value of the key. with expire
  //   * sometimes, this function doesnot working, with exception: error
  //   * @param key key
  //   * @param value value
  //   * @param ttl time expired
  //   * @param pool pool
  //   * @return
  //   */
  //  def setxNative(key: String, value: String, ttl: Long, pool: SSDBPool): Boolean =
  //    (() => pool.withClient(r => r.setx(key, value, ttl))).safeExecWithMessage._1

  /**
   * Get the value related to the specified key
   */
  def get(key: String, pool: SSDBPool): Option[String] =
    pool.safeWithClient { r =>
      val value = r.get(key)
      if (value == null) {
        None
      } else {
        Some(new String(value))
      }
    } match {
      case Some(v) => v
      case None => None
    }

  //  /**
  //    * set value by key.
  //    * set ttl if key does not exists
  //    * @param key key
  //    * @param value value
  //    * @param ttl ttl (by seconds)
  //    * @param pool pool
  //    * @return
  //    */
  //  def setnx(key: String, value: String, ttl: Long, pool: SSDBPool): Boolean =
  //    (() => pool.withClient(r => r.setnx(key, value, ttl))).safeExecWithMessage._1

  def incr(key: String, by: Long, pool: SSDBPool): Option[Long] =
    pool.safeWithClient(_.incr(key, by))

  /**
   * Set the score of the key of a zset.
   */
  def zset(name: String, key: String, score: Long, pool: SSDBPool): Boolean =
    pool.execWithClient(_.zset(name, key, score))._1

  /**
   * Get the score related to the specified key of a zset
   */
  def zget(name: String, key: String, pool: SSDBPool): Option[Long] =
    pool.safeWithClient { r =>
      val value = r.zget(name, key)
      if (value == null) {
        None
      } else {
        Some(value.toLong)
      }
    } match {
      case Some(v) => v
      case None => None
    }

  /**
   * <strong>Important! This method is SLOW for large offset!</strong>
   * <br/>
   * Returns a range of key-score pairs by index range [offset, offset + size). Zrrange iterates in reverse order.
   */
  def zrange(name: String, offset: Int, size: Int, pool: SSDBPool): List[(String, Long)] = {
    if (zsize(name, pool) > 0) {
      pool.safeWithClient { r =>
        val resp = r.zrange(name, offset, size)
        resp.ok() match {
          case true =>
            getKeyValueListFromResp(resp)
          case false =>
            List[(String, Long)]()
        }
      } match {
        case Some(l) =>
          l
        case None =>
          List[(String, Long)]()
      }
    } else {
      List[(String, Long)]()
    }
  }

  /**
   * value must exists
   */
  private def getKeyValueListFromResp(resp: Response): List[(String, Long)] = {
    SafeExecWithTrace {
      val it = resp.keys.iterator()
      val vmap = resp.items
      val l = ListBuffer[(String, Long)]()
      while (it.hasNext) {
        val x = it.next()
        val key = new String(x)
        val value = java.lang.Long.parseLong(new String(vmap.get(x)))
        l.append((key, value))
      }
      l.toList
    } match {
      case Some(v) => v
      case None => List[(String, Long)]()
    }
  }

  /**
   * Return the number of pairs of a zset.
   */
  def zsize(key: String, pool: SSDBPool): Long =
    pool.safeWithClient(_.zsize(key)).safeGetOrElse(0)

  /**
   * return count of keys in [scoreStart, scoreEnd]
   *
   * @param name       name
   * @param scoreStart score start , None for -inf
   * @param scoreEnd   score end, None for +inf
   * @return Option[Count]
   */
  def zcount(name: String, scoreStart: Option[Long], scoreEnd: Option[Long], pool: SSDBPool): Option[Long] = {
    pool.safeWithClient(r =>
      r.zcount(name,
        scoreStart.map(long2Long).orNull,
        scoreEnd.map(long2Long).orNull))
  }

  /**
   * <strong>Important! This method is SLOW for large offset!</strong>
   * <br/>
   * Returns a range of key-score pairs by index range [offset, offset + limit). Zrrange iterates in reverse order.
   */
  def zrrange(name: String, offset: Int, size: Int, pool: SSDBPool): List[(String, Long)] = {
    if (zsize(name, pool) > 0) {
      pool.safeWithClient { r =>
        val resp = r.zrrange(name, offset, size)
        resp.ok() match {
          case true =>
            getKeyValueListFromResp(resp)
          case false =>
            List[(String, Long)]()
        }
      } match {
        case Some(l) =>
          l
        case None =>
          List[(String, Long)]()
      }
    } else {
      List[(String, Long)]()
    }
  }

  /**
   * Increment the number stored at key in a zset by num. <br/>
   *
   * The num argument could be a negative integer. <br/>
   * The old number is first converted to an integer before increment, assuming it was stored as literal integer.
   */
  def zincr(name: String, key: String, by: Long, pool: SSDBPool): Long =
    pool.safeWithClient(_.zincr(name, key, by)) match {
      case Some(v) => v
      case None => 0
    }

  /**
   * Delete specified key of a zset. <br/>
   * take care of the differece between zdel and zclear
   */
  def zdel(name: String, key: String, pool: SSDBPool): Boolean =
    pool.execWithClient(_.zdel(name, key))._1

  /**
   * Delete all keys in a hashmap.
   */
  def hclear(name: String, pool: SSDBPool): Boolean =
    pool.execWithClient(_.hclear(name))._1

  /**
   * Returns the number of items in the queue.
   */
  def qsize(key: String, pool: SSDBPool): Long =
    pool.safeWithClient(_.qsize(key)) match {
      case Some(v) => v
      case None => 0
    }

  /**
   * Returns the first element of a queue.
   */
  def qfront(key: String, pool: SSDBPool): Option[String] =
    pool.safeWithClient(_.qfront(key))

  /**
   * Returns the last element of a queue.
   */
  def qback(key: String, pool: SSDBPool): Option[String] =
    pool.safeWithClient(_.qback(key))

  def qget(key: String, index: Long, pool: SSDBPool): Option[String] =
    pool.safeWithClient(_.qget(key, index))

  /**
   * Returns the element a the specified index(position). <br/>
   * 0 the first element, 1 the second ... -1 the last element.
   */
  def qset(key: String, index: Long, data: String, pool: SSDBPool): Boolean =
    pool.execWithClient(_.qset(key, index, data))._1

  /**
   * This function is an alias of: qpush_back().
   */
  def qpush(key: String, data: String, pool: SSDBPool): Boolean =
    qpush_back(key, data, pool)

  /**
   * Adds an or more than one element to the end of the queue.
   */
  def qpush_back(key: String, data: String, pool: SSDBPool): Boolean = {
    pool.execWithClient(r => r.qpush_back(key, data))._1
  }

  /**
   * Adds one or more than one element to the head of the queue.
   */
  def qpush_front(key: String, data: String, pool: SSDBPool): Boolean =
    pool.execWithClient(_.qpush_front(key, data))._1

  /**
   * Pop out one or more elements from the tail of a queue.
   */
  def qpop_back(name: String, size: Int, pool: SSDBPool): List[String] =
    pool.safeWithClient(r => keysFromResp(r.qpop_back(name, size))).safeGetOrElse(List[String]())

  /**
   * @see qpop_front
   */
  def qpop(name: String, size: Int, pool: SSDBPool): List[String] =
    qpop_front(name, size, pool)

  /**
   * Pop out one or more elements from the head of a queue.
   */
  def qpop_front(name: String, size: Int, pool: SSDBPool): List[String] =
    pool.safeWithClient(r => keysFromResp(r.qpop_front(name, size))).safeGetOrElse(List[String]())

  private def keysFromResp(resp: Response): List[String] = {
    val it = resp.keys.iterator()
    val l = ListBuffer[String]()
    while (it.hasNext) {
      val n = it.next()
      l.append(new String(n))
    }
    l.toList
  }

  def qtrim_back(key: String, size: Int, pool: SSDBPool): Boolean =
    pool.execWithClient(r => r.qtrim_back(key, size))._1

  /**
   * Remove multi elements from the head of a queue.
   */
  def qtrim_front(key: String, size: Int, pool: SSDBPool): Boolean =
    pool.execWithClient(r => r.qtrim_front(key, size))._1

  /**
   * List list/queue names in range (name_start, name_end]
   */
  def qlist(keyStart: String, keyEnd: String, size: Int, pool: SSDBPool): List[String] =
    pool.safeWithClient(r => keysFromResp(r.qlist(keyStart, keyEnd, size))).safeGetOrElse(List[String]())

  /**
   * List list/queue names in range (name_start, name_end]
   */
  def qrlist(keyStart: String, keyEnd: String, size: Int, pool: SSDBPool): List[String] =
    pool.safeWithClient(r => keysFromResp(r.qrlist(keyStart, keyEnd, size))).safeGetOrElse(List[String]())

  /**
   * Returns a portion of elements from the queue at the specified range [offset, offset + limit].
   */
  def qrange(name: String, offset: Int, size: Int, pool: SSDBPool): List[String] =
    pool.safeWithClient(r => keysFromResp(r.qrange(name, offset, size))).safeGetOrElse(List[String]())

  /**
   * Returns a portion of elements from the queue at the specified range [begin, end]. begin and end could be negative.
   */
  def qslice(name: String, offset: Int, size: Int, pool: SSDBPool): List[String] =
    pool.safeWithClient(r => keysFromResp(r.qslice(name, offset, size))).safeGetOrElse(List[String]())

  /**
   * Clear the queue.
   */
  def qclear(name: String, pool: SSDBPool): Boolean =
    pool.execWithClient(r => r.qclear(name))._1

  /**
   * Set the string value in argument as value of the key of a hashmap.
   */
  def hset(name: String, key: String, value: String, pool: SSDBPool): Boolean =
    pool.execWithClient(r => r.hset(name, key, value))._1

  def hexists(name: String, key: String, pool: SSDBPool): Boolean =
    hget(name, key, pool).isDefined

  /**
   * Get the value related to the specified key of a hashmap
   */
  def hget(name: String, key: String, pool: SSDBPool): Option[String] =
    pool.safeWithClient { r =>
      val value = r.hget(name, key)
      if (value == null) {
        None
      } else {
        Some(new String(value))
      }
    } match {
      case Some(v) => v
      case None => None
    }

  /**
   * @param name   name
   * @param prefix prefix
   * @param limit  limit
   * @param pool   pool
   */
  def hscanPrefix(name: String, prefix: String, limit: Int, pool: SSDBPool): List[(String, String)] = {
    val kvs = ArrayBuffer[(String, String)]()
    pool.execWithClient { r =>
      val keyStart = prefix
      val keyEnd = prefixPreProcess(prefix)
      val g = r.hscan(name, keyStart, keyEnd, limit).items.entrySet()
      val it = g.iterator()
      while (it.hasNext) {
        val kv = it.next()
        kvs.append((a2s(kv.getKey), a2s(kv.getValue)))
      }
    }
    kvs.toList
  }

  /**
   * generate a string which is larger than current string
   * abc => abd
   * this function has some bugs if the final character is 0xff,
   * but i dont think it is important in current
   *
   * @param s input string
   * @return
   */
  protected def prefixPreProcess(s: String): String = {
    if (s == null || s.length() == 0) {
      ""
    } else {
      val part = s.splitAt(s.length - 1)
      part._1 + (part._2.charAt(0) + 1).toChar
    }
  }

  /**
   * iter a hmap via (start, +inf]
   * start == "" means (-inf, +inf]
   *
   * @param name      base name
   * @param start     key start
   * @param batchSize batch size
   * @param pool      pool
   * @param body      body , return true to continue
   */
  def hiter(name: String, start: String, batchSize: Int = 1000, pool: SSDBPool)(body: (String, String) => Boolean): Unit = {
    pool.execWithClient { r =>
      var keyStart = start
      var stop = false
      while (!stop) {
        val g = r.hscan(name, keyStart, "", batchSize).items.entrySet()
        if (g.size() == 0) {
          stop = true
        } else {
          val it = g.iterator()
          while (!stop && it.hasNext) {
            val kv = it.next()
            keyStart = a2s(kv.getKey)
            if (!body(keyStart, a2s(kv.getValue))) stop = true
          }
        }
      }
    }
  }

  /**
   * Delete specified key of a hashmap.
   */
  def hdel(name: String, key: String, pool: SSDBPool): Boolean =
    pool.execWithClient(_.hdel(name, key))._1

  def scanPrefixForKey[T](handle: (String, Option[T]) => Unit, prefix: String = "", size: Int = 0, batchSize: Int = 10, data: Option[T] = None, pool: SSDBPool): Boolean = {
    pool.execWithClient { r =>
      var keyStart = prefix
      val keyEnd = prefixPreProcess(prefix)
      var count = 0
      var eof = false
      val rBatchSize = if (batchSize <= 0) 100 else batchSize
      while (!eof && (size == 0 || count < size)) {
        val g = r.scan(keyStart, keyEnd, rBatchSize).items.entrySet()
        val rsize = g.size()
        if (rsize > 0) {
          val it = g.iterator()
          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
          while (it.hasNext && (size == 0 || count < size)) {
            kv = it.next()
            handle(a2s(kv.getKey), data)
            count += 1
          }
          keyStart = a2s(kv.getKey)
        } else {
          eof = true
        }
      }
    }._1
  }

  def delPrefix(prefix: String, pool: SSDBPool) {
    scanPrefix(handle = (k: String, v: String, d: Any) => {
      del(k, pool)
    }, prefix = prefix, size = 0, batchSize = 100, pool = pool)
  }

  def scanPrefix[T](handle: (String, String, Option[T]) => Unit,
    prefix: String = "",
    size: Int = 0,
    batchSize: Int = 10,
    data: Option[T] = None,
    pool: SSDBPool): Boolean = {
    pool.execWithClient { r =>
      var keyStart = prefix
      val keyEnd = prefixPreProcess(prefix)
      var count = 0
      var eof = false
      val rBatchSize = if (batchSize <= 0) 100 else batchSize
      while (!eof && (size == 0 || count < size)) {
        val g = r.scan(keyStart, keyEnd, rBatchSize).items.entrySet()
        val rsize = g.size()
        if (rsize > 0) {
          val it = g.iterator()
          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
          while (it.hasNext && (size == 0 || count < size)) {
            kv = it.next()
            handle(a2s(kv.getKey), a2s(kv.getValue), data)
            count += 1
          }
          keyStart = a2s(kv.getKey)
        } else {
          eof = true
        }
      }
    }._1
  }

  /**
   * Delete specified key.
   */
  def del(key: String, pool: SSDBPool): Boolean = {
    pool.execWithClient(r => r.del(key))._1
  }

  def delValueByKey(key: String, pool: SSDBPool): Boolean = del(key, pool)

  def scanWith(prefix: String = "",
    size: Int = 0,
    batchSize: Int = 100,
    pool: SSDBPool)(body: (String, String) => Unit): Unit = {
    pool.execWithClient { r =>
      var keyStart = prefix
      val keyEnd = prefixPreProcess(prefix)
      var count = 0
      var eof = false
      val rBatchSize = if (batchSize <= 0) 100 else batchSize
      while (!eof && (size == 0 || count < size)) {
        val g = r.scan(keyStart, keyEnd, rBatchSize).items.entrySet()
        val rsize = g.size()
        if (rsize > 0) {
          val it = g.iterator()
          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
          while (it.hasNext && (size == 0 || count < size)) {
            kv = it.next()
            body(a2s(kv.getKey), a2s(kv.getValue))
            count += 1
          }
          keyStart = a2s(kv.getKey)
        } else {
          eof = true
        }
      }
    }._1
  }

  /**
   * List key-value pairs with keys in with prefix : prefix
   */
  def scan[T](handle: (String, String, Option[T]) => Boolean, prefix: String, size: Int = 0, batchSize: Int = 10, data: Option[T] = None, pool: SSDBPool): Boolean = {
    pool.execWithClient { r =>
      var keyStart = prefix
      val keyEnd = prefixPreProcess(prefix)
      var count = 0
      var eof = false
      val rBatchSize = if (batchSize <= 0) 100 else batchSize
      var status = true
      while (!eof && status && (size == 0 || count < size)) {
        val g = r.scan(keyStart, keyEnd, rBatchSize).items.entrySet()
        val rsize = g.size()
        if (rsize > 0) {
          val it = g.iterator()
          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
          while (status && it.hasNext && (size == 0 || count < size)) {
            kv = it.next()
            status = handle(a2s(kv.getKey), a2s(kv.getValue), data)
            count += 1
          }
          keyStart = a2s(kv.getKey)
        } else {
          eof = true
        }
      }
    }._1
  }

  /**
   * iter k-v pairs, key from (may not include) key, score in (scoreStart,scoreEnd]
   *
   * @param name       name
   * @param key        key
   * @param scoreStart min score
   * @param scoreEnd   max score
   * @param size       size
   * @param step       step size
   * @param pool       pool
   * @param body       handle
   */
  def zscan(
    name: String,
    key: String = "",
    scoreStart: Option[Long],
    scoreEnd: Option[Long],
    size: Int = 0,
    step: Int = 10,
    pool: SSDBPool)(body: (String, Long) => Boolean): Boolean = {
    pool.execWithClient { r =>
      var cKey = key
      var cScore: Option[Long] = scoreStart
      var count = 0
      var eof = false
      val rstep = if (step < 1) 100 else step
      while (!eof && (size == 0 || count < size)) {
        //println(s"key start:$cKey, score start : $cScore")
        val g = r.zscan(
          name,
          cKey,
          cScore.map(long2Long).orNull,
          scoreEnd.map(long2Long).orNull,
          rstep).items.entrySet()
        val rsize = g.size()
        if (rsize > 0) {
          val it = g.iterator()
          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
          while (!eof && it.hasNext && (size == 0 || count < size)) {
            kv = it.next()
            cKey = a2s(kv.getKey)
            new String(kv.getValue).safeToLong match {
              case Some(tScore) =>
                cScore = Some(tScore)
                val status = body(cKey, tScore)
                if (!status) {
                  eof = true
                }
              case None =>
                // parse failed
                logger.error(s"[SSDB] parse failed: ${a2s(kv.getValue)}")
                eof = true
            }
            count += 1
          }
        } else {
          eof = true
        }
      }
    }._1
  }

  /**
   * because of it is reversed,
   * start score is larger than end score
   *
   * @param name       name
   * @param key        key
   * @param scoreStart start score
   * @param scoreEnd   end score
   * @param size       size
   * @param step       step size
   * @param pool       pool
   * @param body       handle
   */
  def zrscan(
    name: String,
    key: String = "",
    scoreStart: Option[Long],
    scoreEnd: Option[Long],
    size: Int = 0,
    step: Int = 10,
    pool: SSDBPool)(body: (String, Long) => Boolean): Boolean = {
    pool.execWithClient { r =>
      var cKey = key
      var cScore: Option[Long] = scoreStart
      var count = 0
      var eof = false
      val rstep = if (step < 1) 100 else step
      while (!eof && (size == 0 || count < size)) {
        //println(s"key start:$cKey, score start : $cScore")
        val g = r.zrscan(
          name,
          cKey,
          cScore.map(long2Long).orNull,
          scoreEnd.map(long2Long).orNull,
          rstep).items.entrySet()
        val rsize = g.size()
        if (rsize > 0) {
          val it = g.iterator()
          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
          while (!eof && it.hasNext && (size == 0 || count < size)) {
            kv = it.next()
            cKey = a2s(kv.getKey)
            new String(kv.getValue).safeToLong match {
              case Some(tScore) =>
                cScore = Some(tScore)
                val status = body(cKey, tScore)
                if (!status) {
                  eof = true
                }
              case None =>
                // parse failed
                logger.error(s"[SSDB] parse failed: ${a2s(kv.getValue)}")
                eof = true
            }
            count += 1
          }
        } else {
          eof = true
        }
      }
    }._1
  }

  /**
   * byte array => string <br/>
   * if input is not a byte array, it will return "" in default
   */
  protected def a2s(a: Object) = a match {
    case v: Array[Byte] =>
      new String(v)
    case _ => ""
  }

  //  /**
  //    * <b>WARNING: not fully tested</b>
  //    *
  //    * @param handle    handle
  //    * @param name      name
  //    * @param key       key
  //    * @param scoreMin  min score
  //    * @param scoreMax  max score
  //    * @param size      limitation
  //    * @param batchSize batch size
  //    * @param data      data
  //    * @param pool      pool
  //    * @tparam T type
  //    * @return
  //    */
  //  def zscan[T](handle: (String, Long, Option[T]) => Boolean,
  //               name: String,
  //               key: String,
  //               scoreMin: Long = Long.MinValue,
  //               scoreMax: Long = Long.MaxValue,
  //               size: Int = 0,
  //               batchSize: Int = 10, data: Option[T] = None, pool: SSDBPool): Boolean = {
  //    val sz = {
  //      val rsz = zsize(name, pool)
  //      if (size == 0) rsz
  //      else rsz min size.toLong
  //    }
  //    var ckey = key
  //    pool.execWithClient { r =>
  //      var count = 0L
  //      var eof = false
  //      val rBatchSize = if (batchSize <= 0) 100 else batchSize
  //      var status = true
  //      var cScore = scoreMin
  //      while (!eof && status && count < sz) {
  //        val g = r.zscan(name, ckey, cScore, scoreMin, rBatchSize).items.entrySet()
  //        val rsize = g.size()
  //        if (rsize > 0) {
  //          val it = g.iterator()
  //          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
  //          while (status && it.hasNext && count < sz) {
  //            kv = it.next()
  //            ckey = a2s(kv.getKey)
  //            cScore = new String(kv.getValue).safeToLongOrElse(0L)
  //            status = handle(ckey, cScore, data)
  //            count += 1
  //          }
  //        } else {
  //          eof = true
  //        }
  //      }
  //    }._1
  //  }

  //  /**
  //   * WARNING: it will be slow in large scale ( 1M+ level)
  //   *
  //   * @param handle    handle
  //   * @param name      name
  //   * @param offset    offset
  //   * @param size      limitation
  //   * @param batchSize batch size
  //   * @param data      data
  //   * @param pool      pool
  //   * @tparam T type
  //   * @return
  //   */
  //  def ziter[T](handle: (String, Long, Option[T]) => Boolean,
  //    name: String,
  //    offset: Int = 0,
  //    size: Int = 0,
  //    batchSize: Int = 10, data: Option[T] = None, pool: SSDBPool): Boolean = {
  //    pool.execWithClient { r =>
  //      var count = 0
  //      var eof = false
  //      val rBatchSize = if (batchSize <= 0) 100 else batchSize
  //      var status = true
  //      while (!eof && (size == 0 || count < size)) {
  //        val g = r.zrange(name, offset + count, rBatchSize).items.entrySet()
  //        val rsize = g.size()
  //        if (rsize > 0) {
  //          val it = g.iterator()
  //          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
  //          while (status && it.hasNext && (size == 0 || count < size)) {
  //            kv = it.next()
  //            status = handle(a2s(kv.getKey), new String(kv.getValue).safeToLongOrElse(0L), data)
  //            count += 1
  //          }
  //        } else {
  //          eof = true
  //        }
  //      }
  //    }._1
  //  }

  //  /**
  //    * <b>WARNING: not fully tested</b>
  //    *
  //    * @param handle    handle
  //    * @param name      name
  //    * @param key       key
  //    * @param scoreMin  min score
  //    * @param scoreMax  max score
  //    * @param size      limitation
  //    * @param batchSize batch size
  //    * @param data      data
  //    * @param pool      pool
  //    * @tparam T type
  //    * @return
  //    */
  //  def zrscan[T](handle: (String, Long, Option[T]) => Boolean,
  //                name: String,
  //                key: String,
  //                scoreMin: Long = Long.MinValue,
  //                scoreMax: Long = Long.MaxValue,
  //                size: Int = 0,
  //                batchSize: Int = 10, data: Option[T] = None, pool: SSDBPool): Boolean = {
  //    val sz = {
  //      val rsz = zsize(name, pool)
  //      if (size == 0) rsz
  //      else rsz min size.toLong
  //    }
  //    var ckey = key
  //    pool.execWithClient { r =>
  //      var count = 0L
  //      var eof = false
  //      val rBatchSize = if (batchSize <= 0) 100 else batchSize
  //      var status = true
  //      var cScore = scoreMin
  //      while (!eof && status && count < sz) {
  //        val g = r.zrscan(name, ckey, cScore, scoreMin, rBatchSize).items.entrySet()
  //        val rsize = g.size()
  //        if (rsize > 0) {
  //          val it = g.iterator()
  //          var kv: java.util.Map.Entry[Array[Byte], Array[Byte]] = null
  //          while (status && it.hasNext && count < sz) {
  //            kv = it.next()
  //            ckey = a2s(kv.getKey)
  //            cScore = new String(kv.getValue).safeToLongOrElse(0L)
  //            status = handle(ckey, cScore, data)
  //            count += 1
  //          }
  //        } else {
  //          eof = true
  //        }
  //      }
  //    }._1
  //  }

  /**
   * @see del
   */
  def rm(key: String, pool: SSDBPool): Boolean = del(key = key, pool = pool)

  def delZSetByPrefix(prefix: String, pool: SSDBPool) {
    var count = 0
    zlistIter(prefix = prefix, pool = pool) { k =>
      zclear(k, pool)
      count += 1
      if (count % 10000 == 0) {
        println("remove count :" + count)
      }
      true
    }
    println(s"all removed with prefix : $prefix")
  }

  def zlistIter(
    prefix: String = "",
    size: Int = 0,
    batchSize: Int = 10,
    pool: SSDBPool)(handle: String => Boolean): Boolean = {
    pool.execWithClient { r =>
      var keyStart = prefix
      val keyEnd = prefixPreProcess(prefix)
      var count = 0
      var eof = false
      val rBatchSize = if (batchSize <= 0) 100 else batchSize
      var persist: Boolean = true
      while (!eof && (size == 0 || count < size)) {
        //        println(s"zlist: $keyStart $keyEnd ")
        val g = r.zlist(keyStart, keyEnd, rBatchSize).keys
        val rsize = g.size()
        if (rsize > 0) {
          val it = g.iterator()
          var key: String = null
          while (persist && it.hasNext && (size == 0 || count < size)) {
            key = a2s(it.next())
            persist = handle(key)
            count += 1
          }
          keyStart = key
        } else {
          eof = true
        }
      }
    }
  }._1

  /**
   * Delete all keys in a zset.
   */
  def zclear(name: String, pool: SSDBPool): Boolean =
    pool.execWithClient(_.zclear(name))._1

  def zrlistIter(
    prefix: String = "",
    size: Int = 0,
    batchSize: Int = 10,
    pool: SSDBPool)(handle: String => Boolean): Boolean = {
    pool.execWithClient { r =>
      var keyStart = prefix
      val keyEnd = prefixPreProcess(prefix)
      var count = 0
      var eof = false
      val rBatchSize = if (batchSize <= 0) 100 else batchSize
      var persist: Boolean = true
      while (!eof && (size == 0 || count < size)) {
        //        println(s"zlist: $keyStart $keyEnd ")
        val g = r.zrlist(keyStart, keyEnd, rBatchSize).keys
        val rsize = g.size()
        if (rsize > 0) {
          val it = g.iterator()
          var key: String = null
          while (persist && it.hasNext && (size == 0 || count < size)) {
            key = a2s(it.next())
            persist = handle(key)
            count += 1
          }
          keyStart = key
        } else {
          eof = true
        }
      }
    }
  }._1

  /**
   * Create a new <tt>GenericObjectPool</tt> using the specified values.
   *
   * @param maxActive           the maximum number of objects that can be borrowed from me at one time (see org.apache.commons.pool.impl.GenericObjectPool#setMaxActive)
   * @param whenExhaustedAction the action to take when the pool is exhausted (see org.apache.commons.pool.impl.GenericObjectPool#getWhenExhaustedAction)
   * @param maxWait             the maximum amount of time to wait for an idle object when the pool is exhausted an and
   *                            <i>whenExhaustedAction</i> is org.apache.commons.pool.impl.GenericObjectPool#WHEN_EXHAUSTED_BLOCK (otherwise ignored)
   *                            (see org.apache.commons.pool.impl.GenericObjectPool#getMaxWait)
   * @param host                host of ssdb server
   * @param port                port of ssdb server
   * @param timeout             timeout in ms of ssdb connect, 0 for default and will never timeout
   */
  case class SSDBPool(host: String, port: Int, timeout: Int = 10000, maxActive: Int = 1024, whenExhaustedAction: Byte = WHEN_EXHAUSTED_GROW, maxWait: Long = 10000) extends Awakable {
    // initial log on start
    logger.info(toString)

    val pool = new GenericObjectPool(new SSDBConnectorFactory(host, port, timeout), maxActive, whenExhaustedAction, maxWait)

    /**
     * @param body handle on ssdb request
     * @tparam T optional return type
     * @return
     */
    def safeWithClient[T](body: SSDB => T): Option[T] =
      SafeExecWithTrace(withClient(body))

    /**
     * @param body handle on ssdb request
     * @return
     */
    def execWithClient(body: SSDB => Unit): (Boolean, String) =
      SafeExecWithMessage(withClient[Unit](body))

    /**
     * @param body handle on ssdb request
     * @tparam T return type
     * @return
     */
    def withClient[T](body: SSDB => T): T = {
      val client: SSDB = pool.borrowObject // a timeout exception may comes here
      try {
        body(client)
      } catch {
        case e: SocketTimeoutException =>
          logger.warn(s"[SSDB] [SocketTimeoutException], labeled as INVALID  ${e.getMessage} # $toString")
          client.labelAsInvalid()
          throw e
        case e: ConnectException =>
          logger.warn(s"[SSDB] [ConnectException], labeled as INVALID  ${e.getMessage} # $toString")
          client.labelAsInvalid()
          throw e
        case e: TimeoutException =>
          logger.warn(s"[SSDB] [TimeoutException], labeled as INVALID ${e.getMessage} # $toString")
          client.labelAsInvalid()
          throw e
        case e: Exception =>
          e.printStackTrace()
          logger.error(s"[SSDB] [####Exception####], labeled as INVALID ${e.getMessage} # $toString")
          client.labelAsInvalid()
          throw e
      } finally {
        pool.returnObject(client)
      }
    }

    override def toString = "[SSDB] " + host + ":" + String.valueOf(port)

    // close pool & free resources
    def close() = pool.close()

    /**
     * @param host    host name
     * @param port    port
     * @param timeout timeout in ms
     */
    // pool size
    private class SSDBConnectorFactory(val host: String, val port: Int, val timeout: Int = 0)
      extends PoolableObjectFactory[SSDB] {

      // when we make an object it's already connected
      override def makeObject: SSDB = {
        //logger.info(s"[SSDB] $host:$port connected")
        new SSDB(host, port, timeout)
      }

      // quit & disconnect
      override def destroyObject(rc: SSDB): Unit = {
        //logger.info(s"[SSDB] $host:$port destroied")
        rc.close()
      }

      // noop: we want to have it connected
      override def passivateObject(rc: SSDB): Unit = {
        // post process
      }

      /**
       * Reinitialize an instance to be returned by the pool.
       *
       * @param rc ssdb instance
       */
      override def activateObject(rc: SSDB): Unit = {
        //relink is not necessary here
        //rc.relink()
        if (!rc.isActive) {
          rc.relink()
        }
      }

      override def validateObject(rc: SSDB): Boolean = {
        if (rc.isActive) {
          true
        } else {
          logger.warn("[SSDB] labeled as 'invalid'")
          false
        }
        //rc.isActive // this method is only check the connection method, instead of a "ping"
        //rc.ping()
      }
    }

  }

}

object SSDBClient extends SSDBClient {
  def initSSDBPool(host: String,
    port: Int,
    timeout: Int = 10000,
    maxActive: Int = 1024,
    whenExhaustedAction: Byte = WHEN_EXHAUSTED_GROW,
    maxWait: Long = 10000): SSDBClient.SSDBPool = {
    SSDBClient.SSDBPool(
      host = host,
      port = port,
      timeout = timeout,
      maxActive = maxActive,
      whenExhaustedAction = whenExhaustedAction,
      maxWait = maxWait)
  }
}
