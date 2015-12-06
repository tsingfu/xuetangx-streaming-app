package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.util.Utils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/15.
 */
class FormatLogProcessor extends StreamingProcessor {

  /**处理过滤和属性增强(取值更新，增减字段等)
    *
    * @param rdd
    * @param confMap
    * @return
    */
  override def process(rdd: RDD[String],
              confMap: Map[String, String],
              cacheConfMap: Map[String, String] = null,
              cache_broadcast: Broadcast[Map[String, Map[String, String]]] = null): RDD[String] = {

    val rdd2 = rdd.map(jsonStr=>{
      // json4s 解析json字符串
      try {
        val jValue = parse(jsonStr)

        //获取 username, uid, time, uuid,event_type,agent,origin_referer,spam字段

        //TODO: 更新 user_id
        val event_dict_jValue = jValue \ "event"
        val event_uid = compact(event_dict_jValue \ "uid")
        val event_user_id = compact(event_dict_jValue \ "user_id")
        val event_username = compact(event_dict_jValue \ "username")
        val event_post_email = compact(event_dict_jValue \ "POST" \ "email")
        val context_user_id = compact(jValue \ "context" \ "user_id")

        val user_id_int =
          try{
            if (event_uid.nonEmpty && event_uid.toInt > 0) event_uid.toInt else {
              if (event_user_id.nonEmpty && event_user_id.toInt > 0) event_user_id.toInt else null
            }
          } catch {
            case e: Exception => null
          }

        val time = Utils.strip(compact(jValue \ "time"), "\"")

        val username = Utils.strip(compact(jValue \ "username"), "\"")
        val uuid = Utils.strip(compact(jValue \ "uuid"), "\"")
        val event_type = Utils.strip(compact(jValue \ "event_type"), "\"")
        val agent = Utils.strip(compact(jValue \ "agent"), "\"")
        val origin_referer = Utils.strip(compact(jValue \ "origin_referer"), "\"")
        val spam = Utils.strip(compact(jValue \ "spam"), "\"")
        val host = Utils.strip(compact(jValue \ "host"), "\"")

        val jsonStr2 = "{\"" +
                "username" + "\":\"" + username + "\", \"" +
                "user_id" + "\":" + user_id_int + ", \"" +
                "time" + "\":\"" + time + "\",\"" +
                "uuid" + "\":\"" + uuid + "\",\"" +
                "event_type" + "\":\"" + event_type + "\",\"" +
                "agent" + "\":\"" + agent + "\",\"" +
                "origin_referer" + "\":\"" + origin_referer + "\",\"" +
                "spam" + "\":\"" + spam +
                "host" + "\":\"" + host +
                "\"}"

        //println("= = " * 10 + "[myapp FilterRegisterProcessor.process ] jsonStr2 = " + jsonStr2)
        Some(jsonStr2)

      } catch {
        case e: Exception =>
          None
      }
    })

    rdd2.collect {
      case Some(jsonStr) => jsonStr
    }
  }
}
