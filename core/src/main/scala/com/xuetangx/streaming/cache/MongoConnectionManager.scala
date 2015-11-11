package com.xuetangx.streaming.cache

import com.mongodb.client.MongoCollection
import com.mongodb.{MongoClient, MongoClientURI}
import org.apache.commons.codec.digest.DigestUtils
import org.bson.Document

/**
 * Created by tsingfu on 15/11/5.
 */
object MongoConnectionManager {
/*
  Typically you only create one MongoClient instance for a given database cluster and use it across your application. When creating multiple instances:
          All resource usage limits (max connections, etc) apply per MongoClient instance
  To dispose of an instance, make sure you call MongoClient.close() to clean up resources
*/

  // id = mongoUrlStr
  val mongoClientMap = scala.collection.mutable.Map[String, MongoClient]()

  // id = mongoUrlStr#mongoDb#collection
  val mongoCollectionMap = scala.collection.mutable.Map[String, MongoCollection[Document]]()

  def getCollection(cacheConfMap: Map[String, String]): MongoCollection[Document] = synchronized {
    val mongoUrlStr = cacheConfMap("mongo.connection.url")
    val db = cacheConfMap("mongo.db")

    //TODO: collection 从日志中时间字段解析 yyyy-MM-dd
    val collectionName = cacheConfMap("mongo.collection.url")
    val id = DigestUtils.md5Hex((mongoUrlStr + "#" + db + "#" + collectionName).getBytes("iso-8859-1"))

    mongoCollectionMap.getOrElseUpdate(id, {
      val mongoClient = getClient(mongoUrlStr)
      val mongoDatabase = mongoClient.getDatabase(db)
      val mongoCollection: MongoCollection[Document] = mongoDatabase.getCollection(collectionName)
      mongoCollection
    })
  }

  def getCollection(cacheConfMap: Map[String, String], collectionName: String): MongoCollection[Document] = synchronized {
    val mongoUrlStr = cacheConfMap("mongo.connection.url")
    val db = cacheConfMap("mongo.db")

    //TODO: collection 从日志中时间字段解析 yyyy-MM-dd
    //val collectionName = cacheConfMap("mongo.collection")
    val id = DigestUtils.md5Hex((mongoUrlStr + "#" + db + "#" + collectionName).getBytes("iso-8859-1"))

    mongoCollectionMap.getOrElseUpdate(id, {
      val mongoClient = getClient(mongoUrlStr)
      val mongoDatabase = mongoClient.getDatabase(db)
      val mongoCollection: MongoCollection[Document] = mongoDatabase.getCollection(collectionName)
      mongoCollection
    })
  }


  def getClient(url: String): MongoClient = synchronized {
    mongoClientMap.getOrElseUpdate(url, new MongoClient(new MongoClientURI(url)))
  }
}
