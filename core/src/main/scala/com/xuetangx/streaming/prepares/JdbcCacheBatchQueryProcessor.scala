package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.cache.{JdbcPool, JdbcUtils}
import com.xuetangx.streaming.common.InBatchProcessor
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.ArrayBuffer


/**
 * Created by tsingfu on 15/10/14.
 */
class JdbcCacheBatchQueryProcessor extends StreamingProcessor {

  /**处理过滤和属性增强(取值更新，增减字段等)
    *
    * @param rdd
    * @param confMap
    * @return
    */
  override def process(rdd: RDD[String],
              confMap: Map[String, String],
              cacheConfMap: Map[String, String] = null,
              dataSourceConfMap: Map[String, String] = null): RDD[String] = {

    val tableName = cacheConfMap("tableName")

    val batchLimit = cacheConfMap("batchLimit").trim.toInt

    // 外部关联优化，是否启用条件关联，根据 插件类的 queryOrNot 方法判断
    val cacheQueryConditionEnabled = (cacheConfMap.get("cache.query.condition.enabled") match {
      case Some(x) if x == "true" => "true"
      case _ => "false"
    }).toBoolean

    val batchProcessorInstances = confMap("batchProcessor.class.list").split(",").map(classname => {
      Class.forName(classname.trim).newInstance().asInstanceOf[InBatchProcessor]
    })

    //Note: 考虑到外部缓存 mysql 的查询性能，采用批量查询的方式优化，简化功能，暂支持一个key，同时最好key上有索引
    val keyName = cacheConfMap("keyName")
    val selectKeyNames = cacheConfMap.get("cache.keyName.list") match {
      case Some(x) if x.nonEmpty => x
      case _ => "*"
    }
    val selectClause = "select " + keyName + ", " + selectKeyNames + " from " + tableName

    val rdd2 = rdd.mapPartitions(iter =>{

      val ds = JdbcPool.getPool(cacheConfMap)

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

            val keyValue = Utils.strip(compact(jValue \ keyName), "\"")
            batchKeyArrayBuffer.append(keyValue)

            // 外部关联优化: 1 如果 key 为空，不关联; 2 如果 不为空，根据插件类内部的优化规则选择
            if (keyValue.nonEmpty) {

              val duplicate_in_batch_flag = batchQueryKeyDeDuplicateSet.add(keyValue)

              if (duplicate_in_batch_flag) { //关联key能插入批次集合，批次内不重复，需要查询
                if (cacheQueryConditionEnabled) { //外部关联启用条件查询
                val flagArr = batchProcessorInstances.map(plugin=>{
                    plugin.queryOrNot(jValue, keyValue)
                  })
                  if(flagArr.exists(flag => flag)) { //有存在true的情况，就添加到 batchQueryKeyArrayBuffer
                    batchQueryKeyArrayBuffer.append(keyValue)
                  }
                } else { //如果不启用条件查询
                  batchQueryKeyArrayBuffer.append(keyValue)
                }
              }
              // else {  //关联key不能插入批次集合，批次内重复，不需要重复查询 }
            }


            batchSize += 1
            currentPosition += 1
          }

          if(batchArrayBuffer.length > 0) {
            result = true
            numBatches += 1

            //TODO: 需要处理单/双引号的情况, 不支持含单引号的情况
            val whereClause =
              if (batchQueryKeyArrayBuffer.nonEmpty) {
                " where " + keyName + " in " + batchQueryKeyArrayBuffer.mkString("('", "','", "')")
              } else " where 1 = 0"

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
            val cacheQuerySql = selectClause + whereClause
            val batchQueryResult =
              if(batchQueryKeyArrayBuffer.nonEmpty) {
                JdbcUtils.getQueryResultAsMap2(cacheQuerySql, batchKeyArrayBuffer, keyName, ds)
              } else {
                Map[String, Map[String, String]]()
              }
//            println("= = " * 20 +"[myapp JdbcCacheBatchQueryProcessor.process] batchQueryKeyArrayBuffer.size = " + batchQueryKeyArrayBuffer.size +", cacheQuerySql = " + cacheQuerySql)
//            println("= = " * 20 +"[myapp JdbcCacheBatchQueryProcessor.process] batchQueryResult = " +
//                    batchQueryResult.foreach{case (k, vMap)=> println(k+" -> " * 5 + vMap.mkString("["," ,", "]"))})

            // 方案2批量查询获取缓存后，处理本批次内容
            batchResultArrayBuffer = batchArrayBuffer.zip(batchKeyArrayBuffer).map{ case (jsonStr1, key)=>
              var jsonStr2 = jsonStr1
              batchProcessorInstances.foreach(plugin=>{
                jsonStr2 = plugin.process(jsonStr2, key, batchQueryResult)
              })
              jsonStr2
            }
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
