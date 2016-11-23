package com.argcv.valhalla.client

import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.argcv.valhalla.console.ColorForConsole._
import com.argcv.valhalla.exception.ExceptionHelper.{ SafeExec, SafeExecWithTrace }
import com.argcv.valhalla.utils.Awakable
import com.mongodb.Bytes
import com.mongodb.casbah.Imports
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClientOptions.Builder
import com.mongodb.casbah.commons.TypeImports
import org.bson.types.ObjectId

import scala.concurrent.{ ExecutionContext, Future }
import scala.reflect.ClassTag

/**
 *
 * @author Yu Jing <yu@argcv.com> on 10/31/16
 */
case class MongoDBClient(
  db: String = "admin",
  auth: Option[(String, String, String)] = None, // user pass, authdb
  addrs: Array[(String, Int)] = Array[(String, Int)](("localhost", 27017)), //
  lifeTimeMs: Int = 1000 * 60 * 30, // in ms
  idleTimeMs: Int = 1000 * 60 * 30, // in ms
  maxWaitTimeMs: Int = 1000 * 20, // wait for query: 20 sec
  sockTimeMs: Int = 1000 * 20, // wait for socket connection: 20 sec
  minPerHostFactor: Double = 1.0, // min connection number: cpu cores * factor
  maxPerHostFactor: Double = 3.0, // max connection number: cpu cores * factor
  minPerHost: Int = 1,
  maxPerHost: Int = 32,
  threadsAllowedToBlockForConnectionMultiplier: Int = 1500,
  defaultPreference: ReadPreference = ReadPreference.Nearest) extends Awakable {
  lazy val d: MongoDB = init(addrs, db, auth, primary = false)(db)
  lazy val dPrimary: MongoDB = init(addrs, db, auth, primary = true)(db)

  lazy val nProcessors = MongoDBClient.nProcessors
  lazy val label = MongoDBClient.label

  /**
   * get collection from current db
   *
   * @param name collection name
   */
  def coll(name: String, db: MongoDB = d): MongoCollection = db(name)

  /**
   * loop a collection
   *
   * @param coll       collection
   * @param query      query
   * @param fields     fields
   * @param skip       skip
   * @param limit      limit
   * @param snapshot   use option: snapshot
   * @param persist    use option: persist
   * @param primary    use primary data
   * @param bufferSize buffer size
   * @param body       body
   */
  def parForeach(coll: MongoCollection)(query: DBObject = null,
    fields: Array[String] = null,
    skip: Int = 0,
    limit: Int = 0,
    snapshot: Boolean = false,
    persist: Boolean = false,
    primary: Boolean = false,
    bufferSize: Int = 4096)(body: DBObject => Unit): Unit = {
    val buffMaxSize = bufferSize max 1
    val buffCSize = new AtomicInteger()
    foreach(coll = coll)(
      query = query,
      fields = fields,
      skip = skip,
      limit = limit,
      snapshot = snapshot,
      persist = persist,
      primary = primary
    ) { obj =>
        while (buffCSize.get() > buffMaxSize) ()
        buffCSize.incrementAndGet()
        Future {
          try {
            SafeExecWithTrace(body(obj))
          } finally {
            buffCSize.decrementAndGet()
          }
        }(MongoDBClient.mongoForeachExecutionContext)
      }
    while (buffCSize.get > 0) ()
  }

  /**
   * loop a collection
   *
   * @param coll     collection
   * @param query    query
   * @param fields   fields
   * @param skip     skip
   * @param limit    limit
   * @param snapshot use option: snapshot
   * @param persist  use option: persist
   * @param primary  use primary data
   * @param body     body
   */
  def foreach(coll: MongoCollection)(query: DBObject = null,
    fields: Array[String] = null,
    skip: Int = 0,
    limit: Int = 0,
    snapshot: Boolean = false,
    persist: Boolean = false,
    primary: Boolean = false)(body: DBObject => Unit): Unit = {
    val hcoll = coll.readPrefs(primary)
    var reqFields = MongoDBObject()
    if (fields != null) {
      fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
    }
    var curs = query match {
      case null =>
        fields match {
          case null =>
            hcoll.find()
          case _ =>
            hcoll.find(MongoDBObject(), reqFields)
        }
      case _ =>
        fields match {
          case null =>
            hcoll.find(query)
          case _ =>
            hcoll.find(query, reqFields)
        }
    }
    if (skip > 0) curs.skip(skip)
    if (limit > 0) curs.limit(limit)
    if (snapshot) curs = curs.snapshot()
    if (persist) curs.options_=(Bytes.QUERYOPTION_NOTIMEOUT)
    SafeExecWithTrace(while (curs.hasNext) SafeExecWithTrace(body(curs.next())))
    SafeExecWithTrace(curs.close())
  }

  /**
   * Here is a sample :
   * {{{
   * case class Param(...)
   * val d = Param(values)
   * loopCollection[](
   *  coll = someCol,
   *  handle = (obj: DBObject, d: Param) => {
   *    // process your collection
   *  },
   *  data = d,
   *  persist = true)
   * }}}
   *
   *
   * It will loop this collection
   *
   * @return
   * @author yu
   */
  def loopCollection[T](
    coll: MongoCollection,
    handle: (DBObject, T) => Unit,
    data: T = null, // may ignored
    query: DBObject = null,
    fields: Array[String] = null,
    skip: Int = 0,
    limit: Int = 0,
    snapshot: Boolean = false,
    persist: Boolean = false,
    primary: Boolean = false): Unit = {
    val hcoll = coll.readPrefs(primary)
    var reqFields = MongoDBObject()
    if (fields != null) {
      fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
    }
    var curs = query match {
      case null =>
        fields match {
          case null =>
            hcoll.find()
          case _ =>
            hcoll.find(MongoDBObject(), reqFields)
        }
      case _ =>
        fields match {
          case null =>
            hcoll.find(query)
          case _ =>
            hcoll.find(query, reqFields)
        }
    }
    if (skip > 0) curs.skip(skip)
    if (limit > 0) curs.limit(limit)
    if (snapshot) curs = curs.snapshot()
    if (persist) curs.options_=(Bytes.QUERYOPTION_NOTIMEOUT)
    //    curs.foreach { x => handle(x, data) } // handle exception

    SafeExecWithTrace(while (curs.hasNext) SafeExecWithTrace(handle(curs.next(), data)))
    SafeExecWithTrace(curs.close())
  }

  /**
   * Here is a sample :
   * {{{
   * var count = 0
   * case class Param(...)
   * val d = Param(values)
   * loopCollection[](
   *  coll = someCol,
   *  handle = (obj: DBObject, d: Param) => {
   *    // process your collection
   *
   *    count = count + 1
   *    if(count > 100) false // stop loop
   *    else true // continue loop
   *  },
   *  data = d,
   *  persist = true)
   * }}}
   *
   *
   * loop will broken if you return false
   *
   * @return
   * @author yu
   */
  def loopCollectionWithBreak[T](
    coll: MongoCollection,
    handle: (DBObject, T) => Boolean,
    data: T, query: DBObject = null,
    fields: Array[String] = null,
    skip: Int = 0,
    limit: Int = 0,
    snapshot: Boolean = false,
    persist: Boolean = false,
    primary: Boolean = false): Unit = {
    val hcoll = coll.readPrefs(primary)
    var reqFields = MongoDBObject()
    if (fields != null) {
      fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
    }
    var curs = query match {
      case null =>
        fields match {
          case null =>
            hcoll.find()
          case _ =>
            hcoll.find(MongoDBObject(), reqFields)
        }
      case _ =>
        fields match {
          case null =>
            hcoll.find(query)
          case _ =>
            hcoll.find(query, reqFields)
        }
    }
    if (skip > 0) curs.skip(skip)
    if (limit > 0) curs.limit(limit)
    if (snapshot) curs = curs.snapshot()
    if (persist) curs.options_=(Bytes.QUERYOPTION_NOTIMEOUT)

    var doContinue = true
    try {
      SafeExecWithTrace(
        while (curs.hasNext && doContinue)
          SafeExecWithTrace(doContinue = handle(curs.next(), data)))
    } finally {
      curs.close()
    }
  }

  def findOneByOid(coll: MongoCollection,
    oid: String,
    fields: Array[String] = null,
    primary: Boolean = false): Option[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    oid.oid match {
      case Some(ooid) =>
        fields match {
          case f: Array[String] =>
            var reqFields = MongoDBObject()
            fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
            hcoll.findOne(MongoDBObject("_id" -> ooid), reqFields)
          case null =>
            hcoll.findOneByID(ooid)
        }
      case None =>
        None
    }
  }

  def findOneByKeyValue(coll: MongoCollection,
    key: String, value: Any,
    fields: Array[String] = null,
    primary: Boolean = false): Option[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    fields match {
      case f: Array[String] =>
        var reqFields = MongoDBObject()
        fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
        hcoll.findOne(MongoDBObject(key -> value), reqFields)
      case null =>
        hcoll.findOne(MongoDBObject(key -> value))
    }
  }

  /**
   * find 0 or several by key and value
   *
   * @param coll   collection name
   * @param key    key name
   * @param value  value name
   * @param fields request fields null for all
   * @tparam T type
   * @return
   */
  def findByKeyValue[T: ClassTag](coll: MongoCollection,
    key: String, value: T,
    fields: Array[String] = null,
    primary: Boolean = false): Array[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    try {
      fields match {
        case f: Array[String] =>
          var reqFields = MongoDBObject()
          fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
          hcoll.find(MongoDBObject(key -> value), reqFields).toArray
        case null =>
          hcoll.find(MongoDBObject(key -> value)).toArray
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Array[DBObject]()
    }
  }

  /**
   * find 0 or several by key and value
   *
   * @param coll   collection name
   * @param key    key name
   * @param value  value name
   * @param fields request fields null for all
   * @tparam T type
   * @return
   */
  def findSliceByKeyValue[T: ClassTag](coll: MongoCollection,
    key: String, value: T, offset: Int, size: Int,
    fields: Array[String] = null,
    primary: Boolean = false): Array[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    try {
      fields match {
        case f: Array[String] =>
          var reqFields = MongoDBObject()
          fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
          hcoll.find(MongoDBObject(key -> value), reqFields).skip(offset).limit(size).toArray
        case null =>
          hcoll.find(MongoDBObject(key -> value)).skip(offset).limit(size).toArray
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Array[DBObject]()
    }
  }

  /**
   * find 0 or several by key and value
   *
   * @param coll   collection name
   * @param fields request fields null for all
   * @tparam T type
   * @return
   * @param query query db Object
   */
  def findOneByQuery[T: ClassTag](coll: MongoCollection,
    query: DBObject,
    fields: Array[String] = null,
    primary: Boolean = false): Option[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    try {
      fields match {
        case f: Array[String] =>
          var reqFields = MongoDBObject()
          fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
          hcoll.findOne(query, reqFields)
        case null =>
          hcoll.findOne(query)
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        None
    }
  }

  /**
   * find 0 or several by key and value
   *
   * @param coll   collection name
   * @param fields request fields null for all
   * @return
   * @param query   query db Object
   * @param primary force use primary db
   */
  def findByQuery(coll: MongoCollection,
    query: DBObject,
    fields: Array[String] = null,
    primary: Boolean = false): Array[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    try {
      fields match {
        case f: Array[String] =>
          var reqFields = MongoDBObject()
          fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
          hcoll.find(query, reqFields).toArray
        case null =>
          hcoll.find(query).toArray
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Array[DBObject]()
    }
  }

  /**
   * find 0 or several by key and value
   *
   * @param coll   collection name
   * @param fields request fields null for all
   * @return
   * @param query   query db Object
   * @param primary force use primary db
   */
  def findSliceByQuery[T: ClassTag](coll: MongoCollection,
    query: DBObject, offset: Int, size: Int,
    fields: Array[String] = null,
    primary: Boolean = false): Array[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    try {
      fields match {
        case f: Array[String] =>
          var reqFields = MongoDBObject()
          fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
          hcoll.find(query, reqFields).skip(offset).limit(size).toArray
        case null =>
          hcoll.find(query).skip(offset).limit(size).toArray
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        Array[DBObject]()
    }
  }

  /**
   * find on by ObjectIds
   *
   * @param coll   collection name
   * @param ids    id array
   * @param fields field
   * @return
   */
  def findByOids(coll: MongoCollection,
    ids: Array[String],
    fields: Array[String] = null,
    primary: Boolean = false): Array[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    val oids = ids.flatMap(_.oid).toList
    if (oids.nonEmpty) {
      try {
        if (fields == null) {
          hcoll.find(MongoDBObject("_id" -> MongoDBObject("$in" -> oids))).toArray
        } else {
          var reqFields = MongoDBObject()
          fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
          hcoll.find(MongoDBObject("_id" -> MongoDBObject("$in" -> oids)), reqFields).toArray
        }
      } catch {
        case t: java.lang.IllegalArgumentException =>
          ids.grouped(ids.length / 10 + 1).toArray.par.flatMap(subIds => findByOids(hcoll, subIds, fields)).toArray
        case e: Exception =>
          e.printStackTrace()
          Array[DBObject]()
      }
    } else {
      Array[DBObject]()
    }
  }

  /**
   * find doc by key and values
   *
   * @param coll   collection name
   * @param key    name of keyword
   * @param value  value array of keyworrd
   * @param fields fields to check
   * @tparam T value type
   * @return
   */
  def findByKeyValues[T: ClassTag](coll: MongoCollection,
    key: String, value: Array[T],
    fields: Array[String] = null,
    primary: Boolean = false): Array[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    try {
      fields match {
        case f: Array[String] =>
          var reqFields = MongoDBObject()
          fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
          hcoll.find(MongoDBObject(key -> MongoDBObject("$in" -> value.toList)), reqFields).toArray
        case null =>
          hcoll.find(MongoDBObject(key -> MongoDBObject("$in" -> value.toList))).toArray
      }
    } catch {
      case t: java.lang.IllegalArgumentException =>
        value.grouped(value.length / 10 + 1).toArray.par.flatMap(subIds => findByKeyValues[T](hcoll, key, subIds, fields)).toArray
      case e: Exception =>
        e.printStackTrace()
        Array[DBObject]()
    }
  }

  /**
   * find 0 or several by key and value
   *
   * @param coll   collection name
   * @param key    key name
   * @param value  value name
   * @param fields request fields null for all
   * @tparam T type
   * @return
   */
  def findSliceByKeyValues[T: ClassTag](coll: MongoCollection,
    key: String, value: Array[T], offset: Int, size: Int,
    fields: Array[String] = null,
    primary: Boolean = false): Array[DBObject] = {
    val hcoll = coll.readPrefs(primary)
    try {
      fields match {
        case f: Array[String] =>
          var reqFields = MongoDBObject()
          fields.foreach { x => reqFields = reqFields ++ (x -> 1) }
          hcoll.find(MongoDBObject(key -> MongoDBObject("$in" -> value.toList)), reqFields).skip(offset).limit(size).toArray
        case null =>
          hcoll.find(MongoDBObject(key -> MongoDBObject("$in" -> value.toList))).skip(offset).limit(size).toArray
      }
    } catch {
      case t: java.lang.IllegalArgumentException =>
        value.grouped(value.length / 10 + 1).toArray.par.flatMap(subIds => findByKeyValues[T](hcoll, key, subIds, fields)).toArray
      case e: Exception =>
        e.printStackTrace()
        Array[DBObject]()
    }
  }

  /**
   * remove doc by ObjectID
   *
   * @param coll collection name
   * @param id   id
   * @return
   */
  def rmByOid(coll: MongoCollection, id: String): Boolean = {
    rmByQuery(coll, MongoDBObject("_id" -> new ObjectId(id)))
  }

  /**
   * remove by ObjectIDs
   *
   * @param coll collection name
   * @param ids  id array
   * @return
   */
  def rmByOids(coll: MongoCollection, ids: Array[String]): Boolean = {
    val oids: List[TypeImports.ObjectId] = ids.flatMap(_.oid).toList
    rmByQuery(coll, MongoDBObject("_id" -> MongoDBObject("$in" -> oids)))
  }

  /**
   * remove by one key
   *
   * @param coll collection name
   * @return
   * @param query rm query
   */
  def rmByQuery(coll: MongoCollection, query: DBObject): Boolean =
    SafeExecWithTrace(coll.remove(query)).isDefined

  /**
   * remove by one key
   *
   * @param coll  collection name
   * @param key   keyword name
   * @param value keyword value
   * @return
   */
  def rmByKeyValue(coll: MongoCollection, key: String, value: Any): Boolean =
    rmByQuery(coll, MongoDBObject(key -> value))

  /**
   * safe one object
   *
   * @param coll collection name
   * @param item object to save
   * @return
   */
  def saveObject(coll: MongoCollection, item: DBObject): Option[WriteResult] = {
    SafeExecWithTrace(coll.save(item))
  }

  /**
   * batch insert objects, return bulk write result
   *
   * @param coll  collection name
   * @param items DBObject Array
   * @return
   */
  def batchInsert(coll: MongoCollection, items: Array[DBObject]): Option[BulkWriteResult] = {
    SafeExecWithTrace {
      val builder = coll.initializeOrderedBulkOperation
      items.foreach { x => builder.insert(x) }
      builder.execute()
    }
  }

  /**
   * get total count of collection.
   * this is because of there may something wrong in performance of
   * `count` function `TokuMX`
   *
   * @param coll collection name
   * @return
   */
  def count(coll: MongoCollection): Int = {
    SafeExecWithTrace(coll.count()).getOrElse(0)
  }

  /**
   * create a new object id
   *
   * @return
   */
  def newObjectId(): ObjectId = new ObjectId()

  def isValidObjectId(oid: String) = SafeExec(ObjectId.isValid(oid)).getOrElse(false)

  /**
   * create a new mongo db connection
   *
   * @param addrs   addres
   * @param dbname  db name
   * @param primary primary?
   * @param auth    auth(username and password, or empty)
   */
  private def init(addrs: Array[(String, Int)], dbname: String, auth: Option[(String, String, String)], primary: Boolean = false): MongoClient = {
    val minConnections = (nProcessors * minPerHostFactor).toInt max minPerHost
    val maxConnections = (nProcessors * maxPerHostFactor).toInt min maxPerHost max minConnections
    //def mongoConn(host: String, port: Int, user: String, dbname: String, pass: Array[Char], addrs: Array[(String, Int)]): MongoClient = {
    //println(MONGO_HOST + "\t" + MONGO_PORT + "\t" + MONGO_USER + "\t" + MONGO_DB) // + "\t" + MONGO_PASS.toString)
    if (auth.isDefined) {
      logger.info(s"[$label] auth... user:[${auth.get._1}] db:[$dbname] auth db:[${auth.get._3}]")
    } else {
      logger.info(s"[$label] no-auth.. db:[$dbname] ")
    }
    addrs.foreach(x => logger.info(s"[$label] server:" +
      s" ${x._1.toString.withColor(BLUE)}:${x._2.toString.withColor(YELLOW)}"))
    //val server = new ServerAddress(host, port)
    val credentials: List[MongoCredential] = auth match {
      case Some(a) =>
        List[MongoCredential](MongoCredential.createCredential(a._1, auth.get._3, a._2.toCharArray))
      case None =>
        List[MongoCredential]()
    }
    val optBuilder: Builder = new MongoClientOptions.Builder()
    optBuilder.maxConnectionLifeTime(lifeTimeMs)
    optBuilder.maxConnectionIdleTime(idleTimeMs)
    optBuilder.threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier)
    optBuilder.minConnectionsPerHost(minConnections)
    optBuilder.connectionsPerHost(maxConnections)
    optBuilder.socketTimeout(sockTimeMs) // 60 seconds of socket read
    optBuilder.maxWaitTime(maxWaitTimeMs)
    logger.info(s"[$label] timeouts:  " +
      s"life[$lifeTimeMs](ms), " +
      s"idle[$idleTimeMs](ms), " +
      s"sock[$sockTimeMs](s), " +
      s"wait[$maxWaitTimeMs](s), " +
      s"min connection[$minConnections](fact. $minPerHostFactor / $minPerHost), " +
      s"max connection[$maxConnections](fact. $maxPerHostFactor / $maxPerHost), " +
      s"threads allowed to block for connection multiplier[$threadsAllowedToBlockForConnectionMultiplier]")
    if (primary)
      optBuilder.readPreference(ReadPreference.Primary)
    else
      optBuilder.readPreference(defaultPreference)

    val opt: MongoClientOptions = optBuilder.build()
    val conn: MongoClient = com.mongodb.casbah.Imports.MongoClient(
      replicaSetSeeds = addrs.toList collect {
        case addr: (String, Int) => new ServerAddress(addr._1, addr._2)
      },
      credentials = credentials,
      options = opt)
    conn
  }

  /**
   * @param c collection
   */
  implicit class CollPreferenceOpt(c: MongoCollection) {
    def readPrefs(primaryOnly: Boolean = false): Imports.MongoCollection = {
      if (primaryOnly) {
        coll(c.getName, dPrimary)
      } else
        c
    }
  }

  implicit class SafeToObjectId(id: String) {
    def oid: Option[ObjectId] = SafeExec(new ObjectId(id))
  }

}

object MongoDBClient extends Awakable {
  lazy val label = "MongoDBClient".withColor(GREEN)
  lazy val poolSizeMin = 2
  lazy val poolSizeMax = 256
  lazy val poolSizeFactor = 2.0
  lazy val nProcessors = Runtime.getRuntime.availableProcessors()

  lazy val poolSize = {
    val sz = (Runtime.getRuntime.availableProcessors() * poolSizeFactor).toInt max
      poolSizeMin min
      poolSizeMax
    logger.info(s"[$label] fixed thread pool for foreach " +
      s"min:${poolSizeMin.toString.withColor(GREEN)}, " +
      s"max:${poolSizeMax.toString.withColor(RED)}, " +
      s"factor:${poolSizeFactor.toString.withColor(CYAN)}, " +
      s"real:${sz.toString.withColor(YELLOW)}")
    sz
  }

  lazy implicit val mongoForeachExecutionContext = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(poolSize))
}