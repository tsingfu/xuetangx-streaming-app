package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/15.
 */
class FilterRegisterProcessor extends StreamingProcessor {

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

    val TIME_KEY = "time"
    val USERNAME_KEY = "username"

    val EVENT_UID_KEY = "event_uid"
    val EVENT_UID_KEY1 = "event"
    val EVENT_UID_KEY2 = "uid"

    val UUID_KEY = "uuid"
    val EVENT_TYPE_KEY = "event_type"
    val AGENT_KEY = "agent"
    val ORIGIN_REFERER_KEY = "origin_referer"
    val SPAM_KEY = "spam"

    val rdd2 = rdd.map(jsonStr=>{
      // json4s 解析json字符串
      val jValue = parse(jsonStr)

      //获取 username, uid, time, uuid,event_type,agent,origin_referer,spam字段
      val event_uid = Utils.strip(compact(jValue \ EVENT_UID_KEY1 \ EVENT_UID_KEY2), "\"")
      val time = Utils.strip(compact(jValue \ TIME_KEY), "\"")

      val username = Utils.strip(compact(jValue \ USERNAME_KEY), "\"")
      val uuid = Utils.strip(compact(jValue \ UUID_KEY), "\"")
      val event_type = Utils.strip(compact(jValue \ EVENT_TYPE_KEY), "\"")
      val agent = Utils.strip(compact(jValue \ AGENT_KEY), "\"")
      val origin_referer = Utils.strip(compact(jValue \ ORIGIN_REFERER_KEY), "\"")
      val spam = Utils.strip(compact(jValue \ SPAM_KEY), "\"")

      val jsonStr2 = "{\"" + USERNAME_KEY + "\":\"" + username + "\", \"" +
              EVENT_UID_KEY + "\":\"" + event_uid + "\", \"" +
              TIME_KEY + "\":\"" + time + "\",\"" +
              UUID_KEY + "\":\"" + uuid + "\",\"" +
              EVENT_TYPE_KEY + "\":\"" + event_type + "\",\"" +
              AGENT_KEY + "\":\"" + agent + "\",\"" +
              ORIGIN_REFERER_KEY + "\":\"" + origin_referer + "\",\"" +
              SPAM_KEY + "\":\"" + spam +
              "\"}"

      println("= = " * 20 + "[myapp FilterRegisterProcessor.process ] jsonStr2 = " + jsonStr2)
      jsonStr2
    })

    rdd2.checkpoint()
    rdd2
  }

  
}
