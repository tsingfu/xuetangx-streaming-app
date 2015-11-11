package com.xuetangx.streaming.cache

import org.apache.tomcat.jdbc.pool.DataSource

/**
 * Created by tsingfu on 15/10/15.
 */
object JdbcPool {

  val tomcatJdbcPoolMap = scala.collection.mutable.Map[String, DataSource]()

  def getPool(cacheConfMap: Map[String, String]): DataSource = synchronized {

    val poolId = cacheConfMap("cache.id")
    val jdbcPool = tomcatJdbcPoolMap.getOrElseUpdate(poolId, {

      val driver = cacheConfMap("driver").trim
      val url = cacheConfMap("url")
      val username = cacheConfMap("user").trim
      val password = cacheConfMap("password")
      val tableName = cacheConfMap("tableName")

      val maxActive = cacheConfMap.get("maxActive") match {
        case Some(x) if x.nonEmpty => x.toInt
        case _ => 100
      }
      val initialSize = cacheConfMap.get("initialSize") match {
        case Some(x) if x.nonEmpty => x.toInt
        case _ => 10
      }
      val maxIdle = cacheConfMap.get("maxIdle") match {
        case Some(x) if x.nonEmpty => x.toInt
        case _ => 100
      }
      val minIdle = cacheConfMap.get("minIdle") match {
        case Some(x) if x.nonEmpty => x.toInt
        case _ => 10
      }

      val maxWait = cacheConfMap.get("maxWait") match {
        case Some(x) if x.nonEmpty => x.toInt
        case _ => 10000
      }

      val ds = JdbcUtils.init_dataSource(driver, url, username, password,
        maxActive, initialSize, maxIdle, minIdle, maxWait
      )

      ds
    })

    jdbcPool
  }
}
