package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.common.InBatchProcessor
import org.json4s.jackson.JsonMethods._
import org.json4s._

/**
 * Created by tsingfu on 15/10/15.
 */
class EnhanceApiDeviceInfoProcessor extends InBatchProcessor {

  /** 外部缓存关联时采用批次查询，在(每个/所有)批次查询获取关联信息后，对批次内的每个数据元素进行处理
    * Note: 渠道判断逻辑：
    *  需要关联日志 origin_referer 字段和设备信息(mysql环境api_deviceinfo)中的channel字段，
    *  如果日志中 origin_referer 为空(null or “”)且设备信息channel字段不为空，取设备信息的channel字段值，其他情况取日志中 origin_referer 字段
    * @param record
    * @return
    */
  override def process(record: String,
              key: String,
              cacheData: Map[String, Map[String, String]]): String = {

    val jValue = parse(record)

    //获取json日志中的 origin_referer取值
    val origin_referer =  compact(jValue \ "origin_referer").stripPrefix("\"").stripSuffix("\"")

    //TODO: 测试没有origin_referer的情况
    val origin_referer2 =
      if (origin_referer.isEmpty) {
        //如果日志 origin_referer字段取值为空，
        // 检查缓存中的channel字段取值
        cacheData.get(key) match {
          case Some(x) if x.contains("channel") => // 关联到外部缓存信息
            x("channel")
          case _ => origin_referer
        }
      } else origin_referer

    //更新json日志
    val jValue_update = parse("{\"origin_referer\" : \"" + origin_referer2 + "\"}")
    compact(jValue.merge(jValue_update))
  }


  /** 外部缓存关联时 cache.query.condition.enabled = true 时，进行判断是否需要外部关联
    *
    * @param record
    * @param key
    * @return
    */
  override def queryOrNot(record: Any, key: String): Boolean = {
    var flag = true
    val jValue = record.asInstanceOf[JValue]

    //获取json日志中的 origin_referer取值
    val origin_referer =  compact(jValue \ "origin_referer").stripPrefix("\"").stripSuffix("\"")

    if(origin_referer.nonEmpty) flag = false

    flag
  }

}
