package com.xuetangx.streaming.rules

import com.xuetangx.streaming.StreamingRecordRule
import com.xuetangx.streaming.util.Utils
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/12/2.
 */
class JdbcApiDeviceInfoRecordRule extends StreamingRecordRule {

  val HTTP_PATTERN = "http(s?)://(.*?)/.*".r
  val ORIGIN_REFERER_KEY = "origin_referer"
  val CHANNEL_KEY = "channel"
  val UNKNOWN_ORIGIN_REFERER_VALUE = "unknown"
  val USER_ID_KEY = "user_id"
  val UID_KEY = "uid"

  val SPAM_KEY = "spam"
  val EVENT_KEY = "event"
  val UNKNOWN_SPAM_VALUE = "unknown"

  /** 外部缓存关联时采用批次查询，在(每个/所有)批次查询获取关联信息后，对批次内的每个数据元素进行处理
    * Note: 渠道判断逻辑：
    *  需要关联日志 origin_referer 字段和设备信息(mysql环境api_deviceinfo)中的channel字段，
    *  如果日志中 origin_referer 为空(null or “”)且设备信息channel字段不为空，取设备信息的channel字段值，其他情况取日志中 origin_referer 字段
    * @param record
    * @return
    */
  override def process(record: String,
                       key: String,
                       //cache_data1: Map[String, Map[String, String]] = null,  // cache_broadcast_value
                       //cache_data2: scala.collection.mutable.Map[String, Map[String, String]] = null,  // 批次查询结果累计
                       cache_data2: java.util.concurrent.ConcurrentHashMap[String, Map[String, String]] = null,  // 支持并发
                       cache_data3: Map[String, Map[String, String]] = null  //每个批次查询结果
                       ): String = {

    val key_cache =
//      if (cache_data1 != null && cache_data1.contains(key)) {
//        cache_data1(key)
//      } else
      if (cache_data2 != null && cache_data2.contains(key)) {
        cache_data2.get(key)
      } else if (cache_data3 != null && cache_data3.contains(key)) {
        cache_data3(key)
      } else null

    val jValue = parse(record)
    val origin_referer =  Utils.strip(compact(jValue \ ORIGIN_REFERER_KEY), "\"")
    val spam = Utils.strip(compact(jValue \ SPAM_KEY), "\"")
    val fieldMap = scala.collection.mutable.Map[String, String]()

    if (key_cache == null) {
      val origin_referer2 =
        origin_referer match {
          case HTTP_PATTERN(_, domain) => domain
          case x if x.trim.nonEmpty => x
          case x => UNKNOWN_ORIGIN_REFERER_VALUE
        }
      val spam2 = spam match {
        case x if x.trim.nonEmpty => x
        case _ => UNKNOWN_SPAM_VALUE
      }

      if (origin_referer != origin_referer2) fieldMap.put(ORIGIN_REFERER_KEY, origin_referer2)
      if (spam != spam2) fieldMap.put(SPAM_KEY, spam2)

    } else {
      // 更新 origin_referer
      // 规则：如果日志中 origin_referer 为空(null or “”)且设备信息channel字段不为空，取设备信息的channel字段值，其他情况取日志中 origin_referer 字段
      val origin_referer2 =
        (if (origin_referer.isEmpty) {  //origin_referer为空 先关联外部缓存，如果没有关联到为空字符串 ""
          if (key_cache.contains(CHANNEL_KEY) && key_cache(CHANNEL_KEY) != null && key_cache(CHANNEL_KEY).nonEmpty) key_cache(CHANNEL_KEY) else UNKNOWN_ORIGIN_REFERER_VALUE
        } else {
          origin_referer
        }) match {
          case HTTP_PATTERN(_, domain) => domain
          case x => x
        }

      // 更新 spam
      // 规则：如果用户日志中 spam 为空(null or ””)且设备信息event字段不为空，取设备信息的event，其他情况取用户日志中 spam 的值
      val spam2 =
        if (spam.isEmpty) {
          if (key_cache.contains(EVENT_KEY) && key_cache(EVENT_KEY) != null && key_cache(EVENT_KEY).nonEmpty) key_cache(EVENT_KEY) else UNKNOWN_SPAM_VALUE
        } else {
          spam
        }

      // 更新 user_id
      val user_id = Utils.strip(compact(jValue \ USER_ID_KEY), "\"")
      val user_id_is_valid_number =
        try{
          user_id.toInt
          true
        } catch {
          case ex: java.lang.NumberFormatException =>
            false
        }

      val user_id2 =
        if (user_id_is_valid_number && user_id != "-1") user_id
        else {
          if (key_cache.contains(UID_KEY) && key_cache(UID_KEY) != null && key_cache(UID_KEY).nonEmpty) key_cache(UID_KEY) else user_id
        }

      if (origin_referer != origin_referer2) fieldMap.put(ORIGIN_REFERER_KEY, origin_referer2)
      if (spam != spam2) fieldMap.put(SPAM_KEY, spam2)
      if (user_id != user_id2) fieldMap.put(USER_ID_KEY, user_id2)
    }

    val res =
      if (fieldMap.isEmpty) record
      else {
        val immutableMap = fieldMap.toMap
        val jValue_new = jValue.merge(render(immutableMap))
        compact(jValue_new)
      }
    //TODO: 删除注释
    //println("= = " * 8 + "[myapp JdbcApiDeviceInfoRecordRule.process] updateFields = " + fieldMap.mkString("[", ",", "]"))
    res
  }


  /** 外部缓存关联时 cache.query.condition.enabled = true 时，进行判断是否需要外部关联
    *
    * @param record
    * @param key
    * @return
    */
  override def queryOrNot(record: Any, key: String): Boolean = {
    var flag = true
    val jValue = record.asInstanceOf[JValue]

    //获取json日志中的 origin_referer取值
    val origin_referer =  Utils.strip(compact(jValue \ ORIGIN_REFERER_KEY), "\"")

    //获取json日志中的 spam 取值
    val spam = Utils.strip(compact(jValue \ SPAM_KEY), "\"")

    //TODO: user_id 判断
    if(origin_referer.nonEmpty && spam.nonEmpty) flag = false

    flag
  }

}
