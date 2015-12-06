package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/12/2.
 */
class PlatformLogFormater2 extends StreamingProcessor {

  override def format(rdd: RDD[String], confMap: Map[String, String]): RDD[String] = {

    // 自定义格式化platform日志插件类，在读取数据流后进行格式化
//    stream.transform(rdd => {
      val rdd2 = rdd.map(jsonStr=>{
        // json4s 解析json字符串
        try {
          val jValue = parse(jsonStr)

          //获取 username, uid, time, uuid,event_type,agent,origin_referer,spam字段
          //Note:
          // 注册日志
          // web: uid=正常取值, event.uid = 正常取值， event.uuid 为空, event.sid 为空, username 为空, event.username 没有
          // android: uid = -1, event.uid = '!' + uuid, username 为空， event.username = 特殊字符串
          // androidTV: uid = -1, event.uid = '!' + uuid, username 为空， event.username = 特殊字符串

          //TODO: 更新 user_id
          val uid = Utils.strip(compact(jValue \ "uid"), "\"")
          val event_dict_jValue = jValue \ "event"
          val event_uid = Utils.strip(compact(event_dict_jValue \ "uid"), "\"")
          val event_uuid = Utils.strip(compact(event_dict_jValue \ "uuid"), "\"")
          // val event_user_id = Utils.strip(compact(event_dict_jValue \ "user_id"), "\"")
          val context_user_id = Utils.strip(compact(jValue \ "context" \ "user_id"), "\"")
          val event_username = Utils.strip(compact(event_dict_jValue \ "username"), "\"")
          // val event_post_email = Utils.strip(compact(event_dict_jValue \ "POST" \ "email"), "\"")

          val event_uid_is_valid_number =
            try{
              event_uid.toInt
              true
            } catch {
              case ex: java.lang.NumberFormatException =>
                false
            }

          val context_user_id_is_valid_number =
            try{
              context_user_id.toInt
              true
            } catch {
              case ex: java.lang.NumberFormatException =>
                false
            }

          // 从 event 字段提取， event.uid 能转换为 int 正常，且不为-1，取 event.uid，
          // 如果 context_user_id  能转换为 int 正常，取 event.uid
          // 否则
          //    如果 event.username 不为空，取 event.username,
          //    如果 event_uuid 不为空，取 event_uuid
          //    否则 取 event.uid (!开头代表取值来自 event.uuid)
          val user_id =
            if (event_uid_is_valid_number && event_uid != "-1") event_uid
            else if (context_user_id_is_valid_number) context_user_id
            else {
              if (event_username.nonEmpty) event_username
              else if (event_uuid.nonEmpty) event_uuid
              else event_uid
            }

          val time = Utils.strip(compact(jValue \ "time"), "\"")

          val username = if (event_username.nonEmpty) event_username else Utils.strip(compact(jValue \ "username"), "\"")
          val uuid = Utils.strip(compact(jValue \ "uuid"), "\"")  //Note: web平台 uuid 为空，取 uid 标识

          val event_type = Utils.strip(compact(jValue \ "event_type"), "\"")
          val agent = Utils.strip(compact(jValue \ "agent"), "\"")
          val platform = get_platform(agent)
          val origin_referer = Utils.strip(compact(jValue \ "origin_referer"), "\"") match {
            case "null" => ""
            case x => x
          }
          //          val spam = Utils.strip(compact(jValue \ "spam"), "\"") //BUG: NOTE: 如果设置为null，解析出来是字符串 "null"
          val spam = Utils.strip(compact(jValue \ "spam"), "\"") match {
            // case "null" => null
            case "null" => ""
            case x => x
          }
          val host = Utils.strip(compact(jValue \ "host"), "\"")

          //          val course_id = Utils.strip(compact(jValue \ "event" \ "course_id"), "\"")
          val event_course_id = Utils.strip(compact(jValue \ "event" \ "course_id"), "\"")
          val context_course_id = Utils.strip(compact(jValue \ "context" \ "course_id"), "\"")

          val course_id =
            if (event_course_id.nonEmpty) event_course_id else {
              if (context_course_id.nonEmpty) context_course_id else ""
            }
          /*
                    val jsonStr2 = "{\"" +
                            "username" + "\":\"" + username + "\", \"" +
                            "user_id" + "\":" + user_id + ", \"" +
                            "time" + "\":\"" + time + "\",\"" +
                            "uuid" + "\":\"" + uuid + "\",\"" +
                            "event_type" + "\":\"" + event_type + "\",\"" +
                            "agent" + "\":\"" + agent + "\",\"" +
                            "origin_referer" + "\":\"" + origin_referer + "\",\"" +
                            "spam" + "\":\"" + spam +
                            "host" + "\":\"" + host +
                            "\"}"
          */
          val log_jValue = render(Map("user_id" -> user_id)).merge(
            render(Map[String, String](
              "username" -> username,
              "uuid" -> uuid,
              // "uuid" -> anonymous_uuid,
              "event_type" -> event_type,
              "platform" -> platform,
              "origin_referer" -> origin_referer,
              "spam" -> spam,
              "host" -> host,
              "time" -> time,
              "course_id" -> course_id)
            )
          )

          val res = compact(log_jValue)

          // println("= = " * 20 + "[myapp PlatformLogFormater.format ] res = " + res)
          Some(res)
        } catch {
          case e: Exception =>
            None
        }
      })

      rdd2.collect {
        case Some(jsonStr) => jsonStr
      }

//    })
  }


  /**
   * 由agent获取平台
   * @param agent
   * @return
   */
  def get_platform(agent: String): String = {
    if (agent != null && agent.startsWith("xue tang zai xian android/")){
      "android"
    } else if (agent != null && agent.startsWith("xue tang zai xian androidTV/")) {
      "androidTV"
    } else if (agent != null && (agent.startsWith("xue tang zai xian IOS/") || agent.startsWith("xuetang IOS/"))) {
      "iPhone"
    } else if (agent != null && agent.startsWith("xuetangX-iPad IOS/")) {
      "iPad"
    } else {
      "web"
    }
  }

}
