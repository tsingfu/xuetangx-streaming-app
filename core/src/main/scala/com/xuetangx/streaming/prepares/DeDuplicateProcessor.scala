package com.xuetangx.streaming.prepares

import com.mongodb.BasicDBObject
import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.cache.MongoConnectionManager
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

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

    // 对日志中哪个字段取值进行排重
    val logDeDuplicateKey = confMap("log.deduplicate.key") //示例：user_id
//    val deDuplicateIntervalLabel = confMap("deduplicate.interval.label")
    
    // 约定：spark streaming 中排重与时间相关 正常取值：daily, minutely, hourly, monthly, yearly，：排重集合名取： collectionNamePrefix + durationKey + "_" + logDeDuplicateKey + "_" + "timeStr"
    // 其他取值，不用时间排重，排重集合名取： collectionNamePrefix + durationKey + "_" + logDeDuplicateKey ：示例: deplicate_xyz_user_id (不带时间)
    val deDuplicateTimeDurationList = confMap("deduplicate.interval.labels") 
    //TODO: 支持更细粒度的排重控制
    //val deDuplicateTimeDurationList = confMap("deduplicate.time.duration.list")  //示例： days:1, minutes:1, hours:1, months:1, years: 1 
    val intervalLabels = deDuplicateTimeDurationList.split(",").map(_.trim)
    
    // 从日志哪个字段取查询 mongo 的 collection
    val logCollectionKey = confMap("log.collection.key") //示例：time

    // 在mongo 缓冲中存储id时指定的属性名，默认取 log.deduplicate.key 指定的取值
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
      // time -> user_id_set
      //Note: 此处可能会导致executor内存问题，每个分区任务用完，要清理
      val deDuplicateMap = scala.collection.mutable.HashMap[String, scala.collection.mutable.Set[String]]()

      val collectionNamePrefix = confMap.get("mongo.collection.name.prefix") match {
        case Some(x) if x.nonEmpty => x
        case _ => "deDuplicate_"
      }

      // 遍历 iter，过程中获取是否重复的标记，然后返回增加属性后的iter
      new Iterator[String] {
        override def hasNext: Boolean = {
          val res = iter.hasNext
          if (! res) {
            deDuplicateMap.values.foreach(_.clear())
            deDuplicateMap.clear()
          }
          res
        }

        // TODO: mongo是否有批次查询的接口？
        override def next(): String = {
          val jsonStr = iter.next()
          //println("= = " * 10 + "[myapp debug] jsonStr = " + jsonStr)
          val jValue = parse(jsonStr)

          //获取用于排重的取值
          val deDuplicateValue = Utils.strip(compact(jValue \ logDeDuplicateKey), "\"")

          //TODO: 使用配置方式增加通用性
          // 取时间字段（CST时间字符串中 yyyy-MM-dd）
          
//          val collectionNamePrefix = confMap.get("mongo.collection.name.prefix") match {
//            case Some(x) if x.nonEmpty => x
//            case _ => "deDuplicate_"
//          }
          
          val duplicate_field_map =
            intervalLabels.map(intervalKey=> {
              
              val collectionPrex = collectionNamePrefix + intervalKey + "_" + logDeDuplicateKey
              //示例： deplicate_daily_user_id_20151113, deplicate_minutely_user_id_201511131658, deplicate_hourly_user_id_2015111316
              val collectionName = getCollectionName(jValue, logCollectionKey, collectionPrex, intervalKey)  
              //println("= = " * 10 + "[myapp debug] collectionName1 = " + collectionName)

              val deDuplicateSet = deDuplicateMap.getOrElseUpdate(collectionName, scala.collection.mutable.Set[String]())

              val add_into_deDuplicateSet_flag = deDuplicateSet.add(deDuplicateValue)

              // 处理存在多个 deDuplicateValue 为空的情况： 如果id 为空，无法判断是否重复；默认认为不重复
              val id_duplicate_flag =
                if (deDuplicateValue.isEmpty) 0 // 如果id 为空，不重复
                else if (add_into_deDuplicateSet_flag) {
                  // 添加到 deDuplicateSet，批次中不重复
                  //println("= = " * 10 + "[myapp debug1] add_into_deDuplicateSet " + collectionName + ", add_into_deDuplicateSet_flag = " + add_into_deDuplicateSet_flag + ", not duplicate in batch, deDuplicateValue = " + deDuplicateValue + ", deDuplicateSet = " + deDuplicateSet.mkString("[", ",", "]"))
                  // 缓存数据结构方式1：
                  val coll = MongoConnectionManager.getCollection(cacheConfMap, collectionName)
                  val res = coll.find(new BasicDBObject(mongoCollectionKeyField, deDuplicateValue)).first()
                  // res 为 null 表示不存在mongo缓存中， 0代表不重复，更新外部缓存，1代表重复
                  if (res == null) {
                    //println("= = " * 10 + "[myapp debug2] mongo.find.res = " + res + ", not duplicate in mongo")
                    coll.insertOne(new Document(mongoCollectionKeyField, deDuplicateValue))
                    0
                  } else {
                    //println("= = " * 10 + "[myapp debug3] mongo.find.res = " + res + ", duplicate in mongo")
                    1
                  }
                } else {
                  //不能添加到 deDuplicateSet，批次中重复
                  //println("= = " * 10 + "[myapp debug] add_into_deDuplicateSet true or false = " + add_into_deDuplicateSet + ", duplicate in batch")
                  1
                }

              //添加排重属性
              // val jsonStr_adding = "{\"" + logDeDuplicateKey + "_duplicate_flag" + "\": " + id_duplicate_flag + "}"
              // val jValue_new = jValue.merge(parse(jsonStr_adding))

              //println(Thread.currentThread().getName + "= = " * 10 + "[myapp debug] DeDuplicateProcessor.process add document in collection " + collectionName + ", document = " + logDeDuplicateKey + " _duplicate_flag = " + id_duplicate_flag + " -> " + deDuplicateValue)
              val duplicateFieldName = logDeDuplicateKey + "_" + intervalKey + "_duplicate_flag"
              (duplicateFieldName, id_duplicate_flag)
            }).toMap
          
          val jValue_new = jValue.merge(render(duplicate_field_map))
          val res = compact(jValue_new)
          //println(Thread.currentThread().getName + "= = " * 10 + "[myapp] DeDuplicateProcessor.process res = " + res)
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
  def getCollectionName(jValue: JValue, deDuplicateTimeKey: String, prefix: String, intervalKey: String): String = {
    //deDuplicateTimeKey

    val deDuplicateTimeValue = Utils.strip(compact(jValue \ deDuplicateTimeKey), "\"")
    //println("= = " * 20 + "[myapp debug] mongoCollectionValue = " + deDuplicateTimeValue + ", mongoCollectionKey = " + deDuplicateTimeKey)
    // 对 mongo.collection.key 指定的时间字段取值(格式 yyyy-MM-dd HH:mm:ss)进行加工
    val deDuplicateTimeValue2 = deDuplicateTimeValue.replace("-", "").replace(" ", "").replace(":", "")
    
    intervalKey match {
      case "minutely" => prefix + "_" + deDuplicateTimeValue2.substring(0, 12)  //返回 yyyyMMddHHmm
      case "hourly" =>  prefix + "_" + deDuplicateTimeValue2.substring(0, 10)  //返回 yyyyMMddHH
      case "daily" => prefix + "_" + deDuplicateTimeValue2.substring(0, 8)  //返回 yyyyMMdd
      case "monthly" => prefix + "_" + deDuplicateTimeValue2.substring(0, 6)  //返回 yyyyMM
      case "yearly" => prefix + "_" + deDuplicateTimeValue2.substring(0, 4)  //返回 yyyy
      case _ => 
        println("= = " * 10 + "[myapp] WARNING: durationLabel = " + intervalKey +", duplicateSet name set to " + prefix)
        prefix  //其他条件下，不用根据时间取值
    }
  }
}

