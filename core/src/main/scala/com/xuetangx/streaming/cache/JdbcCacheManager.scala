package com.xuetangx.streaming.cache

import java.util.concurrent.{ConcurrentHashMap=>jcHashMap}
import scala.collection.mutable

/**
 * Created by tsingfu on 15/12/6.
 */
object JdbcCacheManager {

  // 初始化全量 cache, 不变
  // key = cache_id, value = Map[key, key_cache_map]
  val initCaches = mutable.Map[String, Map[String, Map[String, String]]]()
  
  def getCache0(cacheConfMap: Map[String, String]): Map[String, Map[String, String]] = synchronized {
    val cache_id = cacheConfMap.getOrElse("cache.id", "").trim
    
    if (cache_id.isEmpty) null
    else {
      initCaches.getOrElseUpdate(cache_id, {
        val tableName = cacheConfMap("tableName")

        //Note: 考虑到外部缓存 mysql 的查询性能，采用批量查询的方式优化，简化功能，暂支持一个key，同时最好key上有索引
        val keyName = cacheConfMap("keyName")
        val selectKeyNames = cacheConfMap.get("cache.keyName.list") match {
          case Some(x) if x.nonEmpty => x
          case _ => "*"
        }
        val cacheSql = "select " + keyName + ", " + selectKeyNames + " from " + tableName
        val broadcast_enabled = cacheConfMap.getOrElse("broadcast.enabled", "false").toBoolean

        val ds = JdbcPool.getPool(cacheConfMap)
        val res =
          if (broadcast_enabled) {
            JdbcUtils.getQueryResultAsMap2(cacheSql, keyName, ds)
          } else {
            //Map[String, Map[String, String]]()
            null
          }
        res
      })  
    }
  }


  //增量cache，可变
  //val incrementalMutableCaches = mutableMap[String, mutableMap[String, mutableMap[String, String]]]()
  val incrementalMutableCaches = mutable.Map[String, jcHashMap[String, jcHashMap[String, String]]]()
  def getMutableCache1(cacheConfMap: Map[String, String]): jcHashMap[String, jcHashMap[String, String]] = synchronized {
    val cache_id = cacheConfMap.getOrElse("cache.id", "").trim
    if (cache_id.isEmpty) null
    else {
      incrementalMutableCaches.getOrElseUpdate(cache_id, new jcHashMap[String, jcHashMap[String, String]]())
    }
  }

  def updateMutableCache1(key: String, value: String): Unit = {

  }

  //增量cache，不可变
  //val incrementalImmutableCaches = mutableMap[String, mutableMap[String, Map[String, String]]]()
  val incrementalImmutableCaches = mutable.Map[String,
          jcHashMap[String, Map[String, String]]]()
  def getImmutableCache1(cacheConfMap: Map[String, String]): jcHashMap[String, Map[String, String]] = synchronized {
    val cache_id = cacheConfMap.getOrElse("cache.id", "").trim
    if (cache_id.isEmpty) null
    else {
      incrementalImmutableCaches.getOrElseUpdate(cache_id, {
        val map = new jcHashMap[String, Map[String, String]]()
        map
      })
    }
  }

  def updateImmutableCache1(key: String, vMap: Map[String, String]): Unit = {

  }

}
