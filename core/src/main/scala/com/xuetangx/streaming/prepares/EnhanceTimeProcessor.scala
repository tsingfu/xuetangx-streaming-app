package com.xuetangx.streaming.prepares

import java.text.SimpleDateFormat
import java.util.TimeZone

import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.json4s.JsonAST.JField
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/9.
 */
class EnhanceTimeProcessor extends StreamingProcessor {

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

    val timeKey = confMap("timeKeyName")
    val timeKeyAdd = confMap("add.timeKeyName.list")
    val timeIntervalMinutesList = confMap("add.timeKeyInterval.minutes.list")

    val timeKeyIntervalList = timeKey.split(",").map(_.trim).zip(timeIntervalMinutesList.split(",").map(_.trim.toInt))

    rdd.map(jsonStr=>{
      println("= = " * 10 + "[myapp EnhanceTimeProcessor.process]" + jsonStr)
      // json4s 解析json字符串
      val jValue = parse(jsonStr)

      //获取时间字段取值
      val time_old = Utils.strip(compact(jValue \ timeKey),"\"") //发现字符串含有引号
      //        println("= = " * 10 +"E[DEBUG] time_old.length = " + time_old.length +", is startWith(引号)) "+ time_old.startsWith("\"") +", time_old=" + time_old)

      val cstTimeStr = get_cst_time(time_old)

      // 使用指定格式更新时间字段取值，要求：
      // 1统一日志为北京时区时间，如果时间格式不对，置"";
      var jValue_new = jValue transformField {
        case JField(`timeKey`, _) => (timeKey, JString(cstTimeStr))
      }

      // 2新增统计指标时间粒度
      println("= = " * 10 + "[myapp EnhanceTimeProcessor.process] timeKeyIntervalList = " + timeKeyIntervalList.mkString("[", ",", "]")
              +", timeKey = " + timeKey +", timeKeyAdd " + timeKeyAdd +", timeIntervalMinutesList = " + timeIntervalMinutesList)
      timeKeyIntervalList.foreach{case (timeKeyName, timekeyInterval) =>
        val addedTimeValue = get_timeKey(cstTimeStr, timekeyInterval)
        val jsonStr_adding =  "{\"" + timeKeyAdd +"\":\"" + addedTimeValue +"\"}"
        jValue_new = jValue_new.merge(parse(jsonStr_adding))
        println("= = " * 10 + "[myapp EnhanceTimeProcessor.process] jsonStr_adding = " + jsonStr_adding +", jValue_new = " + compact(jValue_new))
      }
      compact(jValue_new)
    }).filter(jsonStr => {
      // json4s 解析json字符串
      val jValue = parse(jsonStr)
      val timeValue = Utils.strip(compact(jValue \ timeKey), "\"")
      timeValue.nonEmpty
    })

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

    //    timeStr match {
    //      case timePattern(d, t, microseonds, sign, h, m) =>
    //        println("= = " * 10 +"E[DEBUG] " + d + ", " + t +", " + microseonds +", " + sign +", " + h + ", " + m)
    //      case _ =>
    //        println("WARNING: found invalid time = " + timeStr)
    //    }

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
    val cstTimeStr = if (formatStr == null) ""
    else {
      val sdf = new SimpleDateFormat(datetimePattern)
      sdf.setTimeZone(TimeZone.getTimeZone(timezoneIDStr))
      sdf.applyPattern(datetimePattern)
      val date = sdf.parse(formatStr)
      //      println("date = " + date)
      sdf.setTimeZone(TimeZone.getTimeZone(DEFAULT_TimeZoneID))
      sdf.format(date)
    }
    //    formatStr
    cstTimeStr
  }

  val DEFAULT_TimeZoneID = "GMT+0800"
  val DEFAULT_datetimePattern = "yyyy-MM-dd HH:mm:ss"
  val DEFAULT_sdf_cst = new SimpleDateFormat(DEFAULT_datetimePattern)
  DEFAULT_sdf_cst.setTimeZone(TimeZone.getTimeZone(DEFAULT_TimeZoneID))
  val DEFAULT_sdf_cst2 = new SimpleDateFormat("yyyyMMddHH")
  DEFAULT_sdf_cst2.setTimeZone(TimeZone.getTimeZone(DEFAULT_TimeZoneID))

  /**
   * 获取特定格式的时间字符串(如2015-09-22 16:01:01)(CST时间)对应的时间范围
   * @param timeStr
   * @param intervalInMin 时间间隔粒度，要求能够被60整除
   * @return
   */
  def get_timeKey(timeStr: String, intervalInMin: Int): String = {
    if (timeStr == null || timeStr.isEmpty) return ""
    else {
      val date = DEFAULT_sdf_cst.parse(timeStr)
      val minutes = date.getMinutes
      val start = scala.math.floor(minutes / intervalInMin).toInt * intervalInMin
      val end = start + intervalInMin
      //      println("start = " + start +", end = " + end)
//      timeStr.substring(0, 10) +":"+ timeStr.substring(11, 14) +
//              (if (start < 10) "0" else "") + start + "-" +
//              (if (end < 10) "0" else "") + end
      DEFAULT_sdf_cst2.format(date) +
              (if (start < 10) "0" else "") + start +
              (if (end < 10) "0" else "") + end
    }
  }
}
