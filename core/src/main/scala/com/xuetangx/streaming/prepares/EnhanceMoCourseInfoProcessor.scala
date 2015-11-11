package com.xuetangx.streaming.prepares

import com.mongodb.BasicDBObject
import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.cache.MongoConnectionManager
import com.xuetangx.streaming.util.{DateFormatUtils, Utils}
import org.apache.spark.rdd.RDD
import org.json4s.jackson.JsonMethods._
import org.json4s.JsonDSL._

/**
 * Created by tsingfu on 15/11/5.
 */
class EnhanceMoCourseInfoProcessor extends StreamingProcessor {

  /**处理过滤和属性增强(取值更新，增减字段等)
    * 关联mongodb，获取课程信息
    *
    * @param rdd
    * @param confMap
    * @return
    */
  override def process(rdd: RDD[String],
              confMap: Map[String, String],
              cacheConfMap: Map[String, String] = null,
              dataSourceConfMap: Map[String, String] = null): RDD[String] = {

    val collectionName = cacheConfMap("mongo.collection.name")
    // TODO: 使 course_id 可配置
    // val fields = cacheConfMap("mongo.collection.field.list")

    rdd.mapPartitions(iter =>{

      // course_id->exist_flag
      val cacheMap = scala.collection.mutable.HashMap[String, Int]()

      val coll = MongoConnectionManager.getCollection(cacheConfMap, collectionName)

      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          val jsonStr = iter.next()
          val jValue = parse(jsonStr)

          //获取 courseId
          val courseId = Utils.strip(compact(jValue \ "course_id"), "\"")

          val exist_flag = cacheMap.getOrElseUpdate(courseId, {
            val mongo_result = coll.find(new BasicDBObject("course_id", courseId)).first()
            // res 为 null 表示mongo缓存没有查到相关信息， 0代表不存在，1代表存在
            if (mongo_result == null) 0 else 1
          })

          //判断课程进度 course_process：要求：该插件依赖 EnhanceMsCourseInfoProcessor
          val course_type = Utils.strip(compact(jValue \ "course_type"), "\"")
          val course_status = Utils.strip(compact(jValue \ "course_status"), "\"")
          val course_start = Utils.strip(compact(jValue \ "course_start"), "\"")  //格式： 2014-01-07 00:00:00
          val course_end = Utils.strip(compact(jValue \ "course_end"), "\"")  //格式： 2014-01-07 00:00:00 ，可能为 null

          val course_start_ms = if (course_start == null) Long.MinValue else DateFormatUtils.dateStr2Ms(course_start)
          val course_end_ms = if (course_start == null) Long.MaxValue else DateFormatUtils.dateStr2Ms(course_end)
          val now = System.currentTimeMillis()

          // 实时日志分析环境，日志是实时，通常 当前时间 > 开课时间，当前时间 < 结课时间
          val course_process =
            if (course_type == "1") {
              if (course_status == "-1") 1 else 0
            } else if (course_type == "0") {
              // TODO: (start is None or start > et[now] or status == "-1")
              if (course_status == "-1 " || course_start == null || course_start_ms > now) -1
              else {
                //TODO: (1 if (end < et[now] or course_map.has_key(course_id)) else 0)
                if (exist_flag == 1 || course_end_ms < now) 1 else 0
              }
            } else -1

          //添加mongo课程信息属性
          // val jsonStr_adding = "{\"course_exist_flag" + "\": " + exist_flag + ", \"course_process\":" + course_process + "}"
          // val jValue_new = jValue.merge(parse(jsonStr_adding))

          val fieldMap_new = Map[String, Int]("course_exist_flag" -> exist_flag, "course_process" -> course_process)
          val jValue_new = jValue.merge(render(fieldMap_new))
          val res = compact(jValue_new)
          // println("= = " * 20 + "[myapp] res = " + res)
          res
        }
      }
    })
  }
}
