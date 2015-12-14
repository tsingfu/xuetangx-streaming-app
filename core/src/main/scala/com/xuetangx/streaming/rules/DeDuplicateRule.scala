package com.xuetangx.streaming.rules

import com.mongodb.BasicDBObject
import com.xuetangx.streaming.StreamingRDDRule
import com.xuetangx.streaming.cache.MongoConnectionManager
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.bson.Document
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable.{Map => mutableMap, Set => mutableSet}
import scala.collection.convert.wrapAsJava._

/**
 * Created by tsingfu on 15/12/1.
 */
class DeDuplicateRule extends StreamingRDDRule {

  val event_group_register_set = Set(
    "common.student.account_created",
    "common.student.account_success",
    "oauth.user.register",
    "oauth.user.register_success",
    "weixinapp.user.register_success",
    "api.user.oauth.register_success",
    "api.user.register",
    "api.user.register_success")

  /** 使用外部缓存识别重复数据，添加标识是否重复的属性
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

    //TODO: 排重功能增强，支持注册日志用户的排重（注册日志用户的排重和用户排重不同，会因为顺序问题，导致排重属性不对）
    // 实现方法：配置 log.deduplicate.keys 设置 keys(分号分隔)#timeKey#timeIntervals(冒号分隔), ...
    // 示例：: user_id#minutely:daily,user_id:event_type#daily,user_id:platform#daily
    //

    // 从日志哪个字段取查询 mongo 的 collection
    val deduplicate_time_Key = confMap("log.collection.key") //示例：time

    val log_deduplicate_keys_time_intervals_list =
      confMap("log.deduplicate.keys").split(",")
              .map(keys_intervals => {
        val keys_intervals_arr = keys_intervals.trim.split("#").map(_.trim)
        assert(keys_intervals_arr.length == 2, "[myapp DeDuplicateRule.process] configuration error: invalid log.deduplicate.keys " + keys_intervals + ", valid log.deduplicate.keys format is keys(分号分隔)#timeKey#timeIntervals(冒号分隔), ...")

        val keys = keys_intervals_arr(0)
        val key_arr = keys.split(":")
        assert(key_arr.length < 3, "[myapp DeDuplicateRule.process] configuration error: invalid log.deduplicate.keys " + keys_intervals +", not support , number of keys > 2")
        val intervals = keys_intervals_arr(1)
        (keys, intervals)
      }) //示例：user_id
    //val log_deduplicate_time_key = confMap("log.collection.key")
    //val log_deduplicate_time_intervals = confMap("deduplicate.interval.labels")


    // 对日志中哪个字段取值进行排重
    //val logDeDuplicateKey = confMap("log.deduplicate.key") //示例：user_id
    // val deDuplicateIntervalLabel = confMap("deduplicate.interval.label")

    // 约定：spark streaming 中排重与时间相关 正常取值：daily, minutely, hourly, monthly, yearly，：排重集合名取： collectionNamePrefix + durationKey + "_" + logDeDuplicateKey + "_" + "timeStr"
    // 其他取值，不用时间排重，排重集合名取： collectionNamePrefix + durationKey + "_" + logDeDuplicateKey ：示例: deplicate_xyz_user_id (不带时间)

    //val deDuplicateTimeDurationList = confMap("deduplicate.interval.labels")
    //TODO: 支持更细粒度的排重控制
    //val deDuplicateTimeDurationList = confMap("deduplicate.time.duration.list")  //示例： days:1, minutes:1, hours:1, months:1, years: 1
    //val intervalLabels = deDuplicateTimeDurationList.split(",").map(_.trim)

    // 排重的数据结构
    // For mongo
    // 方式1：db.collection(yyyy-MM-dd).document(id): 每个排重的id 存储为 一个document ，排重方式：检查document是否存在，不存在添加document; 存在，则设置重复
    // 方式2：db.collection.document(yyyy-MM-dd) : 排重的id作为每个document 的字段，排重方式：检查字段是否存在，不存在更新添加字段; 存在，则设置重复
    // 方式3：db.collection.document(yyyy-MM-dd) : 排重的id存储在每个 document 的 ids 数组字段，排重方式：检查数组中是否存在，不存在更新添加数组元素; 存在，则设置重复
    //TODO: 20151210： 支持2级排重，如 key1: user_id, key2: event_group
    // 更新 mongo 排重数据结构：
    // 方式1： db.collection(collectionNamePrefix + time_interval + key1_name + time_deduplicate).document(key1->key1_value, key2->key2_value)

    //TODO: 内部批次2级排重数据结构：
    // 方式1： deDuplicateMap mutable.Map[
    //          key=mongo.collection_name(collectionNamePrefix + time_interval + key1_name + time_deduplicate),
    //          value=mutable.Map[key1Value, mutable.Map[key2, mutable.Set[key2Value]]]
    //       ]


    val collectionNamePrefix = confMap.get("mongo.collection.name.prefix") match {
      case Some(x) if x.nonEmpty => x
      case _ => "deDuplicate_"
    }

    val rdd2 =
    rdd.mapPartitions(iter => {
      // time -> key_set
      //Note: 此处可能会导致executor内存问题，每个分区任务用完，要清理
      //TODO: 需要处理的问题情景：在处理注册日志前，收到其他日志信息，简单的排重结构会导致数据排重有问题，导致后续统计数据不准
      //val deDuplicateMap = scala.collection.mutable.Map[String, scala.collection.mutable.Set[String]]()
      val deDuplicateMap2 = mutableMap[String, mutableMap[String, mutableMap[String, mutableSet[String]]]]()

      // 遍历 iter，过程中获取是否重复的标记，然后返回增加属性后的iter
      new Iterator[String] {

        override def hasNext: Boolean = {
          val res = iter.hasNext
          if (! res) {
            //deDuplicateMap.values.foreach(_.clear())
            //deDuplicateMap.clear()

            deDuplicateMap2.values.foreach(key1_to_key2Set_map => {
              key1_to_key2Set_map.foreach(key2_to_set_map =>{
                key2_to_set_map._2.values.foreach(_.clear())
                key2_to_set_map._2.clear()
              })
              key1_to_key2Set_map.clear()
            })
            deDuplicateMap2.clear()
          }
          res
        }

        // TODO: mongo是否有批次查询的接口？
        override def next(): String = {
          val jsonStr = iter.next()
          //println("= = " * 10 + "[myapp debug] jsonStr = " + jsonStr)
          val jValue = parse(jsonStr)

          val event_type = Utils.strip(compact(jValue \ "event_type"), "\"")
          //val event_group = Utils.strip(compact(jValue \ "event_group"), "\"")

          val duplicate_field_map_tmp = scala.collection.mutable.Map[String, String]()

          for (keys_intervals_tuple <- log_deduplicate_keys_time_intervals_list){
            val log_deduplicate_keys = keys_intervals_tuple._1.split(":").filter(_.trim.nonEmpty).map(_.trim)
            val log_deduplicate_values = log_deduplicate_keys.map(key => {Utils.strip(compact(jValue \ key), "\"")})

            val time_intervals =  keys_intervals_tuple._2.split(":").filter(_.trim.nonEmpty).map(_.trim)

            //val deDuplicateValue = log_deduplicate_keys.map(key => {Utils.strip(compact(jValue \ key), "\"")}).mkString("#")

            // 在mongo 缓冲中存储id时指定的属性名，默认取 log.deduplicate.key 指定的取值
            // 支持2级key排重，注释
//            val mongoCollectionKeyField = confMap.get("mongo.deduplicate.keyField") match {
//              case Some(x) if x.nonEmpty => x
//              case _ => log_deduplicate_keys.mkString("#")
//            } //示例：id

            //TODO: 发现加了2个排重属性后，性能下降很多，需要改进
            val log_deduplicate_key1 = log_deduplicate_keys(0)
            val log_deduplicate_value1 = log_deduplicate_values(0)
            val log_deduplicate_map = log_deduplicate_keys.zip(log_deduplicate_values).toMap
            val log_deduplicate_key2 = if (log_deduplicate_keys.length > 1) log_deduplicate_keys(1) else ""
            val log_deduplicate_value2 = if (log_deduplicate_keys.length > 1) log_deduplicate_values(1) else ""

            time_intervals.map(intervalKey => {
              //val collectionPrex = collectionNamePrefix + intervalKey + "_" + log_deduplicate_keys.mkString("_")
              val collectionPrex2 = collectionNamePrefix + intervalKey + "_" + log_deduplicate_key1
              //示例： deplicate_daily_user_id_20151113, deplicate_minutely_user_id_201511131658, deplicate_hourly_user_id_2015111316
              //val collectionName = getCollectionName(jValue, deduplicate_time_Key, collectionPrex, intervalKey)
              val collectionName = getCollectionName(jValue, deduplicate_time_Key, collectionPrex2, intervalKey)
              //TODO: 删除注释
              //println("= = " * 10 + "[myapp debug] collectionName1 = " + collectionName)

              //TODO: 需要性能调优，来增强代码灵活通用性
              // event_type 不是 注册类型的日志，不需要通过外部缓存进行排重，设置属性为-1
//              if (log_deduplicate_key_2 == "event_type" && !event_set_register.contains(event_type)) {
//                val duplicateFieldName = log_deduplicate_keys.mkString("_") + "_" + intervalKey + "_duplicate_flag"
//                duplicate_field_map_tmp.put(duplicateFieldName, -1)
//              } else {
              //val deDuplicateSet = deDuplicateMap.getOrElseUpdate(collectionName, mutableSet[String]())
              val deDuplicateSet2 = deDuplicateMap2.getOrElseUpdate(collectionName,
                mutableMap[String, mutableMap[String, mutableSet[String]]]()
              )

              //val add_into_deDuplicateSet_flag = deDuplicateSet.add(deDuplicateValue)
              val add_into_deDuplicateSet_flag =
                if (log_deduplicate_keys.length == 2) {
                  val key2_set =
                    if (deDuplicateSet2.contains(log_deduplicate_value1)) {
                      val key2_to_set_map = deDuplicateSet2(log_deduplicate_value1)

                      if (key2_to_set_map.contains(log_deduplicate_key2)) {
                        key2_to_set_map(log_deduplicate_key2)
                      } else {
                        key2_to_set_map.getOrElseUpdate(log_deduplicate_key2, mutableSet[String]())
                      }
                    } else {
                      val key2_to_set_map = deDuplicateSet2.getOrElseUpdate(log_deduplicate_value1, mutableMap[String, mutableSet[String]]())
                      key2_to_set_map.getOrElseUpdate(log_deduplicate_key2, mutableSet[String]())
                    }
                  key2_set.add(log_deduplicate_value2)
                } else {
                  // log_deduplicate_keys.length == 1
                  val key2_to_set_map = deDuplicateSet2.getOrElseUpdate(log_deduplicate_value1,
                    mutableMap[String, mutableSet[String]]()
                  )
                  if (key2_to_set_map.isEmpty){
                    true
                  } else {
                    false
                  }
                }
                // 处理存在多个 deDuplicateValue 为空的情况： 如果id 为空，无法判断是否重复；默认认为不重复
                val id_duplicate_flag =
                  //if (deDuplicateValue.isEmpty) "0" // 如果id 为空，默认认为不重复
                  if (log_deduplicate_value1.isEmpty) "0" // 如果id 为空，默认认为不重复
                  else if (add_into_deDuplicateSet_flag) {
                    // 添加到 deDuplicateSet，批次中不重复
                    //TODO: 删除注释
                    //println("= = " * 10 + "[myapp debug1] add_into_deDuplicateSet " + collectionName + ", add_into_deDuplicateSet_flag = " + add_into_deDuplicateSet_flag + ", not duplicate in batch, deDuplicateValue = " + deDuplicateValue + ", deDuplicateSet = " + deDuplicateSet.mkString("[", ",", "]"))
                    // 缓存数据结构方式1：
                    val coll = MongoConnectionManager.getCollection(cacheConfMap, collectionName)
                    //val res = coll.find(new BasicDBObject(mongoCollectionKeyField, deDuplicateValue)).first()
                    val res = coll.find(new BasicDBObject(log_deduplicate_map)).first()
                    // res 为 null 表示不存在mongo缓存中， 0代表不重复，更新外部缓存，1代表重复
                    if (res == null) {
                      //TODO: 删除注释
                      //println("= = " * 10 + "[myapp debug2] mongo.find.res = " + res + ", not duplicate in mongo")
                      //coll.insertOne(new Document(mongoCollectionKeyField, deDuplicateValue))
                      coll.insertOne(new Document(log_deduplicate_map))
                      "0"
                    } else {
                      //TODO: 删除注释
                      //println("= = " * 10 + "[myapp debug3] mongo.find.res = " + res + ", duplicate in mongo")
                      "1"
                    }
                  } else {
                    //不能添加到 deDuplicateSet，批次中重复
                    //TODO: 删除注释
                    //println("= = " * 10 + "[myapp debug] add_into_deDuplicateSet true or false = " + add_into_deDuplicateSet_flag + ", duplicate in batch")
                    "1"
                  }

                //添加排重属性
                //println(Thread.currentThread().getName + "= = " * 10 + "[myapp debug] DeDuplicateRule.process add document in collection " + collectionName + ", document = " + logDeDuplicateKey + " _duplicate_flag = " + id_duplicate_flag + " -> " + deDuplicateValue)
                val duplicateFieldName = log_deduplicate_keys.mkString("_") + "_" + intervalKey + "_duplicate_flag"
                duplicate_field_map_tmp.put(duplicateFieldName, id_duplicate_flag)
//              }
            })

            //TODO: 删除注释
//            val platform = Utils.strip(compact(jValue \ "platform"), "\"")
//            val time = Utils.strip(compact(jValue \ "time"), "\"")
//            if (platform != "web"){
//              println(Thread.currentThread().getName + "= = " * 4 + "[myapp] DeDuplicateRule.process " + platform + ", duplicate_field_map = " + duplicate_field_map_tmp.mkString("[", ",", "]") + ", " + log_deduplicate_map.mkString("[", ", ","]") + ", time in log = " + time + ", event_type = " + event_type)
//            }
          }

          val duplicate_field_map = duplicate_field_map_tmp.toMap
          val jValue_new = jValue.merge(render(duplicate_field_map))
          val res = compact(jValue_new)

          //TODO: 删除注释
//          val platform = Utils.strip(compact(jValue \ "platform"), "\"")
//          if (platform != "web" && event_group_register_set.contains(event_type)){
//            println(Thread.currentThread().getName + "= = " * 5 + "[myapp] DeDuplicateRule.process res = " + res)
//          }
          res
        }
      }
    })
    //SparkUtils.persist_rdd(rdd2)
    rdd2
  }


  /** 使用外部缓存识别重复数据，添加标识是否重复的属性
    *
    * @param rdd
    * @return
    */
  def process_old(rdd: RDD[String]
                       //,
                       //cache_broadcast: Broadcast[Map[String, Map[String, String]]] = null,
                       //fun_get_broadcast_value: (String) => Map[String, Map[String, String]]
                       //fun_get_broadcast: () => Broadcast[Map[String, Map[String, Map[String, String]]]]
                              ): RDD[String] = {

    val confMap = conf
    val cacheConfMap = cacheConf

    // 对日志中哪个字段取值进行排重
    val logDeDuplicateKey = confMap("log.deduplicate.key") //示例：user_id
    // val deDuplicateIntervalLabel = confMap("deduplicate.interval.label")

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

    val rdd2 =
      rdd.mapPartitions(iter => {
        // val deDuplicateSet = scala.collection.mutable.Set[String]()
        // time -> user_id_set
        //Note: 此处可能会导致executor内存问题，每个分区任务用完，要清理
        //TODO: 需要处理的问题情景：在处理注册日志前，收到其他日志信息，简单的排重结构会导致数据排重有问题，导致后续统计数据不准
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
            val duplicate_field_map =
              intervalLabels.map(intervalKey=> {

                val collectionPrex = collectionNamePrefix + intervalKey + "_" + logDeDuplicateKey
                //示例： deplicate_daily_user_id_20151113, deplicate_minutely_user_id_201511131658, deplicate_hourly_user_id_2015111316
                val collectionName = getCollectionName(jValue, logCollectionKey, collectionPrex, intervalKey)
                //TODO: 删除注释
                //println("= = " * 10 + "[myapp debug] collectionName1 = " + collectionName)

                val deDuplicateSet = deDuplicateMap.getOrElseUpdate(collectionName, scala.collection.mutable.Set[String]())

                val add_into_deDuplicateSet_flag = deDuplicateSet.add(deDuplicateValue)

                // 处理存在多个 deDuplicateValue 为空的情况： 如果id 为空，无法判断是否重复；默认认为不重复
                val id_duplicate_flag =
                  if (deDuplicateValue.isEmpty) 0 // 如果id 为空，不重复
                  else if (add_into_deDuplicateSet_flag) {
                    // 添加到 deDuplicateSet，批次中不重复
                    //TODO: 删除注释
                    //println("= = " * 10 + "[myapp debug1] add_into_deDuplicateSet " + collectionName + ", add_into_deDuplicateSet_flag = " + add_into_deDuplicateSet_flag + ", not duplicate in batch, deDuplicateValue = " + deDuplicateValue + ", deDuplicateSet = " + deDuplicateSet.mkString("[", ",", "]"))
                    // 缓存数据结构方式1：
                    val coll = MongoConnectionManager.getCollection(cacheConfMap, collectionName)
                    val res = coll.find(new BasicDBObject(mongoCollectionKeyField, deDuplicateValue)).first()
                    // res 为 null 表示不存在mongo缓存中， 0代表不重复，更新外部缓存，1代表重复
                    if (res == null) {
                      //TODO: 删除注释
                      //println("= = " * 10 + "[myapp debug2] mongo.find.res = " + res + ", not duplicate in mongo")
                      coll.insertOne(new Document(mongoCollectionKeyField, deDuplicateValue))
                      0
                    } else {
                      //TODO: 删除注释
                      //println("= = " * 10 + "[myapp debug3] mongo.find.res = " + res + ", duplicate in mongo")
                      1
                    }
                  } else {
                    //不能添加到 deDuplicateSet，批次中重复
                    //TODO: 删除注释
                    //println("= = " * 10 + "[myapp debug] add_into_deDuplicateSet true or false = " + add_into_deDuplicateSet_flag + ", duplicate in batch")
                    1
                  }

                //添加排重属性
                //println(Thread.currentThread().getName + "= = " * 10 + "[myapp debug] DeDuplicateRule.process add document in collection " + collectionName + ", document = " + logDeDuplicateKey + " _duplicate_flag = " + id_duplicate_flag + " -> " + deDuplicateValue)
                val duplicateFieldName = logDeDuplicateKey + "_" + intervalKey + "_duplicate_flag"
                (duplicateFieldName, id_duplicate_flag)
              }).toMap

            //TODO: 删除注释
            val platform = Utils.strip(compact(jValue \ "platform"), "\"")
            val time = Utils.strip(compact(jValue \ "time"), "\"")
            val event_type = Utils.strip(compact(jValue \ "event_type"), "\"")
            if (platform != "web"){
              println(Thread.currentThread().getName + "= = " * 4 + "[myapp] DeDuplicateRule.process " + platform + ", duplicate_field_map = " + duplicate_field_map.mkString("[", ",", "]") + ", " + logDeDuplicateKey + "=" + deDuplicateValue +", time in log = " + time + ", event_type = " + event_type)
            }
            val jValue_new = jValue.merge(render(duplicate_field_map))
            val res = compact(jValue_new)
            //TODO: 删除注释
//            if (platform != "web" && event_group_register_set.contains(event_type)){
//              //in ('common.student.account_created','common.student.account_success','oauth.user.register','oauth.user.register_success','weixinapp.user.register_success','api.user.oauth.register_success','api.user.register','api.user.register_success')
//              println(Thread.currentThread().getName + "= = " * 5 + "[myapp] DeDuplicateRule.process res = " + res)
//            }
            res
          }
        }
      })

    //SparkUtils.persist_rdd(rdd2)
    rdd2
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