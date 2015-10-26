package com.xuetangx.streaming.util

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/17.
 */
object Json4sUtils {

  def addField(jValue: JValue, key: String, value: String): JValue = {
    jValue.merge(parse("{\"" + key +"\":\"" +  value + "\"}"))
  }

  def addFields(jValue: JValue, kvMap: Map[String, String]): JValue = {
    val jsonStr = kvMap.map{case (k, v) => "\"" +k +"\":\"" + v +"\""}.mkString("{", ",", "}")
    jValue.merge(parse(jsonStr))
  }

  def updateField(jValue: JValue, key: String, value: String): JValue = {
    jValue.transformField {
      case JField(`key`, _) => (key, JString(value))
    }
  }

  def removeField(jValue: JValue, key: String): JValue = {
    jValue.removeField {
      case JField(`key`, _) => true
      case _ => false
    }
  }


/*
  def filterField(jValue: JValue, key: String): JValue = {
    val res = jValue.filterField {
      case JField(`key`, _) => false
      case _ => true
    }


    res //filterField 返回类型不是Jvalue
  }
*/


}
