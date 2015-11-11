package com.xuetangx.streaming.common

import com.mongodb.BasicDBObject
import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.cache.MongoConnectionManager
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

/**
 * Created by tsingfu on 15/11/9.
 */
class DeDuplicateProcessor extends StreamingProcessor {

  /** 使用外部缓存识别重复数据，添加标识是否重复的属性
    *
    * @param rdd
    * @param confMap
    * @param cacheConfMap
    * @param dataSourceConfMap
    * @return
    */
  override def process(rdd: RDD[String],
                       confMap: Map[String, String],
                       cacheConfMap: Map[String, String] = null,
                       dataSourceConfMap: Map[String, String] = null): RDD[String] = {

    // 对日志中那个字段取值进行排重
    val logDeDuplicateKey = confMap("log.deduplicate.key") //示例：user_id
    val deDuplicateIntervalLabel = confMap("deduplicate.interval.label")
    // 从日志哪个字段取查询 mongo 的 collection
    val logCollectionKey = confMap("log.collection.key") //示例：time

    val mongoCollectionKeyField = confMap.get("mongo.deduplicate.keyField") match {
      case Some(x) if x.nonEmpty => x
      case _ => logDeDuplicateKey
    } //示例：id

    // 排重的数据结构
    // For mongo
    // 方式1：db.collection(yyyy-MM-dd).document(id): 每个排重的id 存储为 一个document ，排重方式：检查document是否存在，不存在添加document; 存在，则设置重复
    // 方式2：db.collection.document(yyyy-MM-dd) : 排重的id作为每个document 的字段，排重方式：检查字段是否存在，不存在更新添加字段; 存在，则设置重复
    // 方式3：db.collection.document(yyyy-MM-dd) : 排重的id存储在每个 document 的 ids 数组字段，排重方式：检查数组中是否存在，不存在更新添加数组元素; 存在，则设置重复

    rdd.mapPartitions(iter => {
      // val deDuplicateSet = scala.collection.mutable.Set[String]()
      // 每日排重: time_d -> user_id_set
      val deDuplicateMap = scala.collection.mutable.HashMap[String, scala.collection.mutable.Set[String]]()

      // 遍历 iter，过程中获取是否重复的标记，然后返回增加属性后的iter
      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        // TODO: mongo是否有批次查询的接口？
        override def next(): String = {
          val jsonStr = iter.next()
          //println("= = " * 10 + "[myapp debug] jsonStr = " + jsonStr)
          val jValue = parse(jsonStr)

          //获取用于排重的取值
          val deDuplicateValue = Utils.strip(compact(jValue \ logDeDuplicateKey), "\"")

          //TODO: 使用配置方式增加通用性
          // 取时间字段（CST时间字符串中 yyyy-MM-dd）
          val collectionNamePrefix = confMap("mongo.collection.name.prefix")
          val collectionName = getCollectionName(jValue, logCollectionKey, collectionNamePrefix + logDeDuplicateKey + "_")
          //println("= = " * 10 + "[myapp debug] collectionName1 = " + collectionName)

          val deDuplicateSet = deDuplicateMap.getOrElseUpdate(collectionName, scala.collection.mutable.Set[String]())

          //TODO: 处理存在多个 deDuplicateValue 为空的情况： 如果id 为空，无法判断是否重复；默认不重复
          val add_into_deDuplicateSet = if (deDuplicateValue.nonEmpty) deDuplicateSet.add(deDuplicateValue) else false
          val id_duplicate_flag =
            if (deDuplicateValue.isEmpty) 0 // 如果id 为空，不重复
            else if (add_into_deDuplicateSet) {
              // 添加到 deDuplicateSet，批次中不重复
              //println("= = " * 10 + "[myapp debug] add_into_deDuplicateSet true or false = " + add_into_deDuplicateSet + ", not duplicate in batch")
              // 缓存数据结构方式1：
              val coll = MongoConnectionManager.getCollection(cacheConfMap, collectionName)
              val res = coll.find(new BasicDBObject(mongoCollectionKeyField, deDuplicateValue)).first()
              // res 为 null 表示不存在mongo缓存中， 0代表不重复，更新外部缓存，1代表重复
              if (res == null) {
                coll.insertOne(new Document(mongoCollectionKeyField, deDuplicateValue))
                0
              } else {
                //println("= = " * 10 + "[myapp debug] mongo.find.res = " + res + ", duplicate in mongo")
                1
              }
            } else {
              //println("= = " * 10 + "[myapp debug] add_into_deDuplicateSet true or false = " + add_into_deDuplicateSet + ", duplicate in batch")
              1
            } //不能添加到 deDuplicateSet，批次中重复

          //添加排重属性
          // val jsonStr_adding = "{\"" + logDeDuplicateKey + "_duplicate_flag" + "\": " + id_duplicate_flag + "}"
          // val jValue_new = jValue.merge(parse(jsonStr_adding))

          //println("= = " * 10 + "[myapp debug] DeDuplicateProcessor.process add  " + logDeDuplicateKey + " _duplicate_flag = " + id_duplicate_flag + ", deDuplicateValue = " + deDuplicateValue)
          val duplicateKeyName = logDeDuplicateKey + (if (deDuplicateIntervalLabel.isEmpty) "" else "_" + deDuplicateIntervalLabel) + "_duplicate_flag"
          val jValue_new = jValue.merge(render(Map[String, Int](duplicateKeyName -> id_duplicate_flag)))
          val res = compact(jValue_new)
          //println("= = " * 10 + "[myapp] DeDuplicateProcessor.process res = " + res)
          res
        }
      }
    })
  }

  /**
   * 获取用于排重的id，存储mongo时使用的 collection 名， 默认指定的名字
   * @param jValue
   * @param deDuplicateTimeKey
   * @param prefix
   * @return
   */
  def getCollectionName(jValue: JValue, deDuplicateTimeKey: String, prefix: String): String = {
    deDuplicateTimeKey
  }

}

