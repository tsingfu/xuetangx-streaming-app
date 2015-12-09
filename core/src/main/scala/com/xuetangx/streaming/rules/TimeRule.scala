package com.xuetangx.streaming.rules

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.xuetangx.streaming.StreamingRDDRule
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/11/29.
 */
class TimeRule extends StreamingRDDRule {

  /**处理过滤和属性增强(取值更新，增减字段等)
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

    val confMap = this.conf
    // val cacheConfMap = this.cacheConf
    // val cache_broadcast = this.cache_broadcast

    val timeKey = confMap("timeKeyName") //日志中时间字段名
    val timeIntervalMinutesList = confMap("add.timeKeyInterval.minutes.list")  //时间区间间隔
    val timeKeyPrefix = confMap("add.timeKey.prefix")  //新增时间属性名前缀

    rdd.map(jsonStr=>{
      // json4s 解析json字符串
      val jValue = parse(jsonStr)

      //获取时间字段取值
      val time = Utils.strip(compact(jValue \ timeKey),"\"") //发现字符串首尾含有引号，去掉
      //println("= = " * 10 + "[myapp TimeRule.process]" + jsonStr + ", time = " + time)
      val cstTimeStr = get_cst_time(time)

      val res =
        if (cstTimeStr == null || cstTimeStr.isEmpty) { //时间不符合规则，过滤
          //TODO: 删除测试
          //println("= = " * 10 + "[myapp TimeRule.process] found invalid cstTimeStr, time = " + time)
          None
        } else {

          val fieldMap = scala.collection.mutable.Map[String, String]()
          // 使用指定格式更新时间字段取值，要求：
          // 1统一日志为北京时区时间，如果时间格式不对，置"";
          //          var jValue_new = jValue transformField {
          //            case JField(`timeKey`, _) => (timeKey, JString(cstTimeStr))
          //          }

          fieldMap.put(timeKey, cstTimeStr)

          // 2新增统计指标时间粒度 start_date, end_date
          //println("= = " * 10 + "[myapp TimeRule.process] timeKeyIntervalList = " + timeIntervalMinutesList.mkString("[", ",", "]") + ", timeKey = " + timeKey + ", timeIntervalMinutesList = " + timeIntervalMinutesList)
          timeIntervalMinutesList.split(",").map(_.trim).foreach { case timekeyInterval =>
            val (start_date_str, end_date_str) = get_timeKey(cstTimeStr, timekeyInterval.toInt)
            fieldMap.put(timeKeyPrefix + timekeyInterval + "_start", start_date_str)
            fieldMap.put(timeKeyPrefix + timekeyInterval + "_end", end_date_str)
          }

          // val jValue_new = jValue.merge(render(fieldMap)) // 问题：merge后，jValue_new 丢失其他属性
          // val jValue_new = jValue.merge(render(fieldMap.toMap))  // 提示 required： org.json4s.JValue
          val json_add_map = fieldMap.toMap
          val jValue_new = jValue.merge(render(json_add_map))
          //println("= = " * 10 + "[myapp TimeRule.process] user_id = " + Utils.strip(compact(jValue \ "user_id"),"\"") +
          //        ", jsonStr_adding = " + json_add_map.mkString("[", ",", "]"))
          Some(jValue_new)
        }
      res
    }).collect {
      case Some(jValue) => compact(jValue)
    }
  }


  /**
   * 对特定格式的时间字符串(如2015-09-22T16:01:01.910651+00:00，但不确定时区) 格式化 yyyy-MM-dd HH:mi:ss (CST北京时间)
   * @param timeStr
//   * @param intervalMin
 * @return
   */
  def get_cst_time(timeStr: String): String = {

    val timePattern = """"{0,1}(\d{4}-\d{2}-\d{2})[T| ](\d{2}:\d{2}:\d{2})\.{0,1}(\d{6}){0,1}(\+){0,1}(\d{2}){0,1}:{0,1}(\d{2}){0,1}"{0,1}""".r
    var datetimePattern = "yyyy-MM-dd HH:mm:ss"
    var minStr: String = null
    var timezoneIDStr = "GMT+0800"
    val DEFAULT_TimeZoneID = "GMT+0800"

    val formatStr = timeStr match {
      case timePattern(d, t, microseonds, sign, h, m) =>
        assert(d != null)
        assert(t != null)
        val dtStr = if (microseonds == null) {
          d + " " + t
        } else if (sign == null) {
          datetimePattern += ".S"
          d + " " + t + microseonds.substring(0, 3)
        } else if (h == null) {
          datetimePattern += ".S Z"
          //          timezoneIDStr = "GMT+0800"
          d + " " + t + "." + microseonds.substring(0, 3) + " +0800"
        } else if (m == null) {
          datetimePattern += ".S Z"
          timezoneIDStr = "GMT" + sign + h + "00"
          d + " " + t + "." + microseonds.substring(0, 3) + " " + sign + h + "00"
        } else {
          datetimePattern += ".S Z"
          timezoneIDStr = "GMT" + sign + h + m
          d + " " + t + "." + microseonds.substring(0, 3) + " " + sign + h + m
        }
        minStr = t.substring(3, 5)
        dtStr
      case _ => //时间字段格式不对，置null
        println("WARNING: format_time() failed to format time = " + timeStr)
        //        timeStr
        null
    }
    //    println("formatStr = " + formatStr +", datetimePattern = " + datetimePattern)
    val cstTimeStr =
      if (formatStr == null) ""
      else {
        try {
          val sdf = new SimpleDateFormat(datetimePattern)
          sdf.setTimeZone(TimeZone.getTimeZone(timezoneIDStr))
          sdf.applyPattern(datetimePattern)
          val date = sdf.parse(formatStr)
          // println("date = " + date)
          sdf.setTimeZone(TimeZone.getTimeZone(DEFAULT_TimeZoneID))
          sdf.format(date)
        } catch {
          case ex: Exception => ""
        }
      }
    cstTimeStr
  }

  val DEFAULT_TimeZoneID = "GMT+0800"
  val DEFAULT_datetimePattern = "yyyy-MM-dd HH:mm:ss"
  val DEFAULT_sdf_cst = new SimpleDateFormat(DEFAULT_datetimePattern)
  DEFAULT_sdf_cst.setTimeZone(TimeZone.getTimeZone(DEFAULT_TimeZoneID))

  val DEFAULT_sdf_cst_hour = new SimpleDateFormat("yyyy-MM-dd HH")
  DEFAULT_sdf_cst_hour.setTimeZone(TimeZone.getTimeZone(DEFAULT_TimeZoneID))
  val DEFAULT_sdf_cst_second = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  DEFAULT_sdf_cst_second.setTimeZone(TimeZone.getTimeZone(DEFAULT_TimeZoneID))


  /**
   * 获取特定格式的时间字符串(如2015-09-22 16:01:01)(CST时间)对应的时间范围
   * @param timeStr
   * @param intervalInMin 时间间隔粒度，要求能够被60整除
   * @return
   */
  def get_timeKey(timeStr: String, intervalInMin: Int): (String, String) = {
    if (timeStr == null || timeStr.isEmpty)
      ("", "")
    else {
      val date = DEFAULT_sdf_cst.parse(timeStr)
      val minutes = date.getMinutes
      val start = scala.math.floor(minutes / intervalInMin).toInt * intervalInMin
      // val end = start + intervalInMin
      val end = start + intervalInMin - 1 //修正 2015-10-26 14:60:00 不符合 yyyy-MM-dd HH:mm:ss的问题 2015-10-26 14:59:59

      /*
            DEFAULT_sdf_cst_hour.format(date) +
                    (if (start < 10) "0" else "") + start +
                    (if (end < 10) "0" else "") + end
      */

      val hourStr = DEFAULT_sdf_cst_hour.format(date)

      //      (hourStr + (if (start < 10) "0" else "") + start + "00",
      //              hourStr + (if (end < 10) "0" else "") + end + "00")
      (hourStr +":" + (if (start < 10) "0" else "") + start + ":00",
              hourStr +":" + (if (end < 10) "0" else "") + end + ":59")

    }
  }
}
