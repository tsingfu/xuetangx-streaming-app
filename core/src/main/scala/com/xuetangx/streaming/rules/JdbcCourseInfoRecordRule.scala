package com.xuetangx.streaming.rules

import com.xuetangx.streaming.StreamingRecordRule
import com.xuetangx.streaming.util.DateFormatUtils
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/12/2.
 */
class JdbcCourseInfoRecordRule extends StreamingRecordRule {

  /** 外部缓存关联时采用批次查询，在(每个/所有)批次查询获取关联信息后，对批次内的每个数据元素进行处理
    * course_id 关联 mysql.course_meta_course 中 course_id, course_type, owner, status, start, end
    *
    * @param record
    * @return
    */
  override def process(record: String,
                       key: String,
                       cache_data1: Map[String, Map[String, String]] = null,  // cache_broadcast_value
                       cache_data2: scala.collection.mutable.Map[String, Map[String, String]] = null,  // 批次查询结果累计
                       cache_data3: Map[String, Map[String, String]] = null  //每个批次查询结果
                       ): String = {
    val jValue = parse(record)
    //取 courseId
//    val courseId = Utils.strip(compact(jValue \ "course_id"), "\"")

    val key_cache =
      if (cache_data1 != null && cache_data1.contains(key)) {
        cache_data1(key)
      } else if (cache_data2 != null && cache_data2.contains(key)) {
        cache_data2(key)
      } else if (cache_data3 != null && cache_data3.contains(key)) {
        cache_data3(key)
      } else null

    //val jsonStr_adding =
    val addFieldMap =
      if (key_cache != null) { // 存在课程信息
        val course_type = key_cache("course_type")
        val course_owner = key_cache("owner")
        val course_status = key_cache("status")
        val course_start = key_cache("start")
        val course_end = key_cache("end")

        val course_start_ms = if (course_start == null || course_start.isEmpty) Long.MinValue else {
          try {
            // yyyy-MM-dd HH:mi:ss
            if (course_start.length == 19) DateFormatUtils.dateStr2Ms(course_start)
            else if (course_start.length > 19) DateFormatUtils.dateStr2Ms(course_start.substring(0, 19))
            else Long.MinValue
          } catch {
            case ex: Exception =>
              //println("= = " * 8 + "[myapp JdbcCourseInfoRecordRule.process] found invalid course_start = " + course_start +", record = " + record)
              Long.MinValue
          }
        }
        val course_end_ms = if (course_end == null || course_end.isEmpty) Long.MaxValue else {
          try {

            if (course_start.length == 19) DateFormatUtils.dateStr2Ms(course_end)
            else if (course_start.length > 19) DateFormatUtils.dateStr2Ms(course_end.substring(0, 19))
            else Long.MaxValue
          } catch {
            case ex: Exception =>
              //println("= = " * 8 + "[myapp JdbcCourseInfoRecordRule.process] found invalid course_end = " + course_end +", record = " + record)
              Long.MaxValue
          }
        }
        val now = System.currentTimeMillis()

        //TODO: 确认 course_process 逻辑是否正确
        val course_process =
          if (course_type == "1") {
            if (course_status == "-1") 1 else 0
          } else if (course_type == "0") {
            // TODO: (start is None or start > et[now] or status == "-1")
            if (course_status == "-1 " || course_start == null || course_start_ms > now) -1
            else {
              //TODO: (1 if (end < et[now] or course_map.has_key(course_id)) else 0)
              if (course_end_ms < now) 1 else 0
            }
          } else -1

        //s"""{"course_type":"$course_type", "course_owner":"$course_owner", "course_status":"$course_status", "course_start":"$course_start", "course_end":"$course_end", "course_process":$course_process}"""
        Map[String, String]("course_type" -> course_type, "course_owner" -> course_owner, "course_status" -> course_status, "course_start" -> course_start, "course_end" -> course_end, "course_process" -> course_process.toString)
      } else {  // mysql没有查到，表示不存在
        //s"""{"course_type":null, "course_owner":null, "course_status":null, "course_start":null, "course_end":null, "course_process":null}"""
        //s"""{"course_type":"", "course_owner":"", "course_status":"", "course_start":"", "course_end":null, "course_process":""}"""
        Map[String, String]("course_type" -> "", "course_owner" -> "", "course_status" -> "", "course_start" -> "", "course_end" -> "", "course_process" -> "")
      }

    //TODO: 删除测试
    //println("= = " * 10 + "[myapp JdbcCourseInfoRecordRule.process] addFieldMap = " + addFieldMap.mkString("[", ",", "]"))
    val jValue_new = jValue.merge(render(addFieldMap))
    val res = compact(jValue_new)
    // println("= = " * 10 + "[myapp JdbcCourseInfoRecordRule.process] res = " + res)
    res
  }
}