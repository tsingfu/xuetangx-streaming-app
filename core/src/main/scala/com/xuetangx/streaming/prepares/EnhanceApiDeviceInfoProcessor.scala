package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.common.InBatchProcessor
import org.json4s.jackson.JsonMethods._
import org.json4s._

/**
 * Created by tsingfu on 15/10/15.
 */
class EnhanceApiDeviceInfoProcessor extends InBatchProcessor {

  val HTTP_PATTERN = "http(s?)://(.*?)/.*".r

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

    //没有origin_referer为空的情况
    val origin_referer2 =
      if (origin_referer.isEmpty) {  //origin_referer为空 先关联外部缓存，如果没有关联到为空字符串 ""
        //如果日志 origin_referer字段取值为空，
        // 检查缓存中的channel字段取值
        cacheData.get(key) match {
          case Some(x) if x.contains("channel") => // 关联到外部缓存信息
            if (x("channel") == null || x("channel").isEmpty) "unknown channel" else x("channel")
          case _ =>
            // origin_referer
            "unknown channel"
        }
      } else { //origin_referer 非空，取url的 domain； baidu的收索的关键字目前看不到，如果没有取到domain，取原始值
        origin_referer match {
          case HTTP_PATTERN(_, domain) => domain
          case x => x
        }
      }

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
