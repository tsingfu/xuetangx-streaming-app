package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.common.InBatchProcessor
import com.xuetangx.streaming.util.Utils
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

/**
 * Created by tsingfu on 15/10/15.
 */
class EnhanceApiDeviceInfoProcessor extends InBatchProcessor {

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
              cacheData: Map[String, Map[String, String]]): String = {

    val cache = cacheData.get(key) match {
      case Some(x) => x
      case _ => null
    }

    val jValue = parse(record)
    val origin_referer =  Utils.strip(compact(jValue \ ORIGIN_REFERER_KEY), "\"")
    val spam = Utils.strip(compact(jValue \ SPAM_KEY), "\"")
    val fieldMap = scala.collection.mutable.Map[String, String]()

    if (cache == null) {
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
          if (cache.contains(CHANNEL_KEY) && cache(CHANNEL_KEY) != null && cache(CHANNEL_KEY).nonEmpty) cache(CHANNEL_KEY) else UNKNOWN_ORIGIN_REFERER_VALUE
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
          if (cache.contains(EVENT_KEY) && cache(EVENT_KEY) != null && cache(EVENT_KEY).nonEmpty) cache(EVENT_KEY) else UNKNOWN_SPAM_VALUE
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
          if (cache.contains(UID_KEY) && cache(UID_KEY) != null && cache(UID_KEY).nonEmpty) cache(UID_KEY) else user_id
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
    //println("= = " * 8 + "[myapp EnhanceApiDeviceInfoProcessor.process] updateFields = " + fieldMap.mkString("[", ",", "]") + ", res = " + res)
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
