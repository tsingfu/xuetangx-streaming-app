package com.xuetangx.streaming.rules

/**
 * Created by tsingfu on 15/12/1.
 */
import com.xuetangx.streaming.StreamingRDDRule
import com.xuetangx.streaming.cache.{JdbcCacheManager, JdbcPool, JdbcUtils}
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer

class JdbcCacheRule extends StreamingRDDRule {

  /**处理过滤和属性增强(取值更新，增减字段等)
    *
    * @param rdd
    * @return
    */
  override def process(rdd: RDD[String]
                       //,
                       //cache_broadcast: Broadcast[Map[String, Map[String, String]]] = null,
                       //fun_get_broadcast_value: (String) => Map[String, Map[String, String]]
                       //fun_get_broadcast: () => Broadcast[Map[String, Map[String, Map[String, String]]]]
                              ): RDD[String] = {

    val confMap = conf
    val cacheConfMap = cacheConf

    val tableName = cacheConfMap("tableName")
    val batchLimit = cacheConfMap("batchLimit").trim.toInt

    // 外部关联优化，是否启用条件关联，根据 插件类的 queryOrNot 方法判断
    val cacheQueryConditionEnabled = (cacheConfMap.get("cache.query.condition.enabled") match {
      case Some(x) if x == "true" => "true"
      case _ => "false"
    }).toBoolean

    //Note: 考虑到外部缓存 mysql 的查询性能，采用批量查询的方式优化，简化功能，暂支持一个key，同时最好key上有索引
    val keyName = cacheConfMap("keyName")
    val log_key_name = cacheConfMap.getOrElse("log.key.name", "").trim match {
      case x if x.nonEmpty => x
      case _ => keyName
    }
    val selectKeyNames = cacheConfMap.get("cache.keyName.list") match {
      case Some(x) if x.nonEmpty => x
      case _ => "*"
    }
    val selectClause = "select " + keyName + ", " + selectKeyNames + " from " + tableName

    //val broadcast_enabled = cacheConfMap.getOrElse("broadcast.enabled", "false").toBoolean
    //val cache_id = cacheConf.getOrElse("cache.id", "").trim

    //该方法有问题, 每个 task 反序列后，需要反复进行 broadcast操作，非常慢
    //val cache_broadcast_value = if (broadcast_enabled  && cache_broadcast != null) cache_broadcast.value else null
    //测试1, 该方法有问题, 每个 task 反序列后，需要反复进行 broadcast操作，非常慢
    //val cache_broadcast_value = fun_get_broadcast_value(cache_id)
    // 测试2： 该方法有问题, 每个 task 反序列后，需要反复进行 broadcast操作，非常慢
    //val cache_broadcast_value = fun_get_broadcast().value.getOrElse(cache_id, null)
    // 测试3：伪 broadcast， 每个 task 反序列后，需要反复进行 broadcast操作，非常慢； 原因：不能出现在rdd map操作之外中，否者会遇到每个 task 序列和反序列 cache 的问题
    //val cache_broadcast_value = if (broadcast_enabled && cache_id.nonEmpty) JdbcCacheManager.getCache(cacheConfMap) else null


    val cache_reserved = cacheConf.getOrElse("cache.reserved", "true").toBoolean

    val rdd2 = rdd.mapPartitions(iter =>{
      val ds = JdbcPool.getPool(cacheConfMap)
      // 不能放在 rdd mapPartitions 之外，否者会遇到每个 task 序列和反序列 cache 的问题
      //val cache_data = scala.collection.mutable.Map[String, Map[String, String]]()
      val cache_data = JdbcCacheManager.getImmutableCache1(cacheConfMap)

      new Iterator[String] {
        private[this] var currentElement: String = _
        private[this] var currentPosition: Long = -1

        private[this] var batchPosition: Int = -1
        private[this] val batchArrayBuffer = ArrayBuffer[String]()
        private[this] var batchResultArrayBuffer: ArrayBuffer[String] = _

        //TODO: 考虑到外部缓存 mysql 的查询性能，采用批量查询的方式优化，简化功能，暂支持一个key，同时最好key上有索引
        private[this] val batchKeyArrayBuffer = ArrayBuffer[String]()
        private[this] val batchQueryKeyArrayBuffer = ArrayBuffer[String]()
        private[this] var numBatches: Long = 0

        private[this] val batchQueryKeyDeDuplicateSet = scala.collection.mutable.Set[String]()

        override def hasNext: Boolean = {
          val flag = (batchPosition != -1 && batchPosition < batchArrayBuffer.length) || (iter.hasNext && batchNext())
          flag
        }

        override def next(): String = {
          batchPosition += 1
          batchResultArrayBuffer(batchPosition - 1)
        }

        def batchNext(): Boolean = {
          var result = false

          batchArrayBuffer.clear()
          batchKeyArrayBuffer.clear()
          batchQueryKeyDeDuplicateSet.clear()

          var batchSize: Int = 0
          //记录批次处理的数据
          while (iter.hasNext && (batchSize < batchLimit)) {
            currentElement = iter.next()
            batchArrayBuffer.append(currentElement)

            //Note: 特殊的处理
            //  json形式
            //  查询时指定范围 id
            val jValue = parse(currentElement)

            //val keyValue = Utils.strip(compact(jValue \ keyName), "\"")
            val keyValue = Utils.strip(compact(jValue \ log_key_name), "\"")
            batchKeyArrayBuffer.append(keyValue)

            // 外部关联优化: 1 如果 key 为空，不关联; 2 如果 不为空，根据插件类内部的优化规则选择
            if (keyValue.nonEmpty) {
              //先检查 cache_broadcast_value 中是否存在： 该方法有问题, 每个 task 反序列后，需要反复进行 broadcast操作，非常慢
              //   如果不存在，进行批量查询，之后进行处理；如果存在，不进行外部查询，直接根据 cache_broadcast_value 中的关联信息处理
//              if (broadcast_enabled && cache_broadcast_value.contains(keyValue)) {
//                //cache_broadcast_value 命中不需要外部查询
//                //TODO: 删除注释
//                println("= = " * 10 + "[myapp JdbcCacheRule.process] cache optimization1: cache_broadcast_value cache hit with " + keyName + " = " + keyValue)
//                null
//              } else
            if (cache_reserved  && cache_data.contains(keyValue)){
                // cache_broadcast_value 不命中；如果保存了之前查询的 cache_data 结果，检查 cache_data
                // cache_data 命中，不需要外部查询
                //TODO: 删除注释
                //println("= = " * 10 + "[myapp JdbcCacheRule.process] cache optimization2: cache_reserved is enabled, cache_data hit with " + keyName + " = " + keyValue)
                null
              } else {
                // 如果 cache_broadcast_value/cache_data 都没有命中，进行批次内数据排重检查，如果已经添加到 batchQueryKeyDeDuplicateSet 中，不需要再查询
                val duplicate_in_batch_flag = batchQueryKeyDeDuplicateSet.add(keyValue)
                if (duplicate_in_batch_flag) {
                  //关联key能插入批次集合，批次内不重复，需要查询
                  if (cacheQueryConditionEnabled) {
                    //外部关联启用条件查询
//                    val flagArr = batchProcessorInstances.map(plugin => {
//                      plugin.queryOrNot(jValue, keyValue)
//                    })
                    val flagArr = recordRules.map(rule => {
                      rule.queryOrNot(jValue, keyValue)
                    })
                    if (flagArr.exists(flag => flag)) {
                      //有存在true的情况，就添加到 batchQueryKeyArrayBuffer
                      batchQueryKeyArrayBuffer.append(keyValue)
                      //TODO: 删除注释
                      //println("= = " * 10 + "[myapp JdbcCacheRule.process] cache optimization3: cacheQueryConditionEnabled is enabled, need query for " + keyName + " = " + keyValue)
                    }
                  } else {
                    //如果不启用条件查询
                    batchQueryKeyArrayBuffer.append(keyValue)
                    //TODO: 删除注释
                    //println("= = " * 10 + "[myapp JdbcCacheRule.process] cache optimization4: cacheQueryConditionEnabled is disabled, need query for " + keyName + " = " + keyValue)
                  }
                }
                // else {  //关联key不能插入批次集合，批次内重复，不需要重复查询 }
              }
            }

            batchSize += 1
            currentPosition += 1
          }

          if(batchArrayBuffer.length > 0) {
            result = true
            numBatches += 1

            //TODO: 需要处理单/双引号的情况, 不支持含单引号的情况
            //批量查询外部缓存记录到map，
            /*
                        // 方案1: cacheMap保存整个分区的缓存信息，可以避免批次内的重复记录的查询
                        // Note：内存中会记录整个分区的所有缓存信息, 可能出现内存问题；
                        batchKeyArrayBuffer.zip(queryResultMapArr).foreach{case (k, vMap) => cacheMap.put(k, vMap)}
            */

            // 方案2：可以只保留本批次的缓存，然后批次对批次数据处理，
            // Note: 可以避免内存问题，但存在重复查询
            // 暂使用方案2
            //TODO: 处理没有关联信息的情况
            // Note: 要求记录的key和cache数据具有一对一的关系，如果没有关联信息，batchQueryResult中没有

            val batchQueryResult =
              if(batchQueryKeyArrayBuffer.nonEmpty) {
                val whereClause = " where " + keyName + " in " + batchQueryKeyArrayBuffer.mkString("('", "','", "')")
                val cacheQuerySql = selectClause + whereClause
                //TODO: 删除注释
                //println("= = " * 10 + "[myapp JdbcCacheRule.process] batch cacheQuerySql = " + cacheQuerySql)
                JdbcUtils.getQueryResultAsMap2(cacheQuerySql, keyName, ds)
              } else {
                Map[String, Map[String, String]]()
              }

            if (cache_reserved){
              for ((key, vcache) <-batchQueryResult){
                cache_data.put(key, vcache)
              }
            }

            // 方案2批量查询获取缓存后，处理本批次内容
            batchResultArrayBuffer = batchArrayBuffer.zip(batchKeyArrayBuffer).map{
              case (jsonStr1, key)=>
                var jsonStr2 = jsonStr1
                recordRules.foreach(rule => {
                  //TODO: 删除注释
                  //println("= = " * 10 + "[myapp JdbcCacheRule.process] rule = " + rule.rule_name)
                  //jsonStr2 = rule.process(jsonStr2, key, cache_broadcast_value, cache_data, batchQueryResult)
                  jsonStr2 = rule.process(jsonStr2, key, cache_data, batchQueryResult)
                })
                jsonStr2
            }
          } else {

          }

          //// 方案1：批量查询获取缓存后，处理本批次内容

          batchSize = 0
          batchPosition = 0
          result
        }
      }
    })
    rdd2
  }
}
