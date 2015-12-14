package com.xuetangx.streaming.rules


import com.xuetangx.streaming.StreamingRecordRule
import com.xuetangx.streaming.util.Utils
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/12/2.
 */
class JdbcAuthUserInfoRecordRule extends StreamingRecordRule {

  /** 外部缓存关联时采用批次查询，在(每个/所有)批次查询获取关联信息后，对批次内的每个数据元素进行处理
    * course_id 关联 mysql.course_meta_course 中 course_id, course_type, owner, status, start, end
    *
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
    // 获取用户注册日期信息

    val addField =
      if (key_cache != null) {
        // 如果 cache 中存在相关 key 的信息
//        val cache = cacheData(key)
        if (key_cache.contains("date_joined") && key_cache("date_joined") != null && key_cache("date_joined").nonEmpty) {
          ("date_joined", key_cache("date_joined"))
        } else {
          ("date_joined", "")
        }
      } else {
        // 如果 cache 中不存在相关 key 的信息
        ("date_joined", "")
      }

    //TODO: 删除测试
    //println("= = " * 10 + "[myapp JdbcAuthUserInfoRecordRule.process] addField = " + Map(addField).mkString("[", ",", "]"))

    // 新增属性，是否当天注册，当天注册 1，非当天
    val jValue_new = jValue.merge(render(addField))
    val res = compact(jValue_new)
    //TODO: 删除测试
    //println("= = " * 10 + "[myapp JdbcAuthUserInfoRecordRule.process] res = " + res)
    res
  }


  /** 外部缓存关联时 cache.query.condition.enabled = true 时，进行判断是否需要外部关联
    *
    * @param record
    * @param key
    * @return
    */
  override def queryOrNot(record: Any, key: String): Boolean = {
    //var flag = true
    val jValue = record.asInstanceOf[JValue]

    val userId =  Utils.strip(compact(jValue \ "user_id"), "\"")
    val user_id_is_valid_number =
      try{
        userId.toInt
        true
      } catch {
        case ex: java.lang.NumberFormatException =>
          false
      }

    // flag = if (user_id_is_valid_number && (userId != "-1")) true else false
    // flag
    if (user_id_is_valid_number && (userId != "-1")) true else false
  }
}