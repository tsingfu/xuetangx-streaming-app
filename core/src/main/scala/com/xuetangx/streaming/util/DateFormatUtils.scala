package com.xuetangx.streaming.util

import java.sql.Date
import java.text.SimpleDateFormat

/**
 * Created by tsingfu on 15/10/29.
 */
object DateFormatUtils {

  val DEFAULT_PATTERN = "yyyy-MM-dd HH:mm:ss"
  val defaultSDF = new SimpleDateFormat(DEFAULT_PATTERN)
  val sdfTmp = new SimpleDateFormat()

  //毫秒转换为字符串
  def dateMs2Str(dateMs: Long): String ={
    defaultSDF.format(new Date(dateMs))
  }

  //字符串转换为毫秒
  def dateStr2Ms(dateStr: String): Long ={
    defaultSDF.parse(dateStr).getTime
  }


  def dateMs2Str(dateMs: Long, pattern: String): String ={
    sdfTmp.applyPattern(pattern)
    sdfTmp.format(new Date(dateMs))
  }

  def dateStr2Ms(dateStr: String, pattern: String): Long ={
    sdfTmp.applyPattern(pattern)
    sdfTmp.parse(dateStr).getTime
  }

}
