package com.xuetangx.streaming.rules

import java.util

import com.xuetangx.streaming.StreamingRDDRule
import com.xuetangx.streaming.output.elasticsearch.ESUtils
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.indices.IndexMissingException
//import org.elasticsearch.script.ScriptService.ScriptType  //0.90.11版本没有该类
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JField
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/12.
 */
class ESOutputRule extends StreamingRDDRule {

  /**输出统计指标到elasticsearch
    *
    * @param rdd 含有统计信息指标的rdd，
    *            格式：统计指标的维度(targetKeys) 统计指标的时间维度(time_key，如每分钟 time_minute_1 ), 统计指标取值value,
    *             需要附加的其他信息：
    */
  override def output(rdd: RDD[String]): RDD[String] = {

    val RAW_DATA_KEY = "raw_data"
    val confMap = outputConf
    val idx = confMap("index")
    val typ = confMap("type")
    //val idKeys = confMap("origin.statistic.key").split(",") //取值对应targetKeysList中各统计指标维度名，逗号分隔
    val valueKey = confMap("value.key")
    val valueType = confMap("value.type")
    //val excludingKeys = Set(valueKey)

    val esAddUpdateMethod = confMap.get("add.update.method") match {
      case Some(x) if x.nonEmpty => x
      case _ => "getAndUpdate"
    }

    val esScriptEnable = if(esAddUpdateMethod == "script") {
      confMap.get("script.enabled") match {
        case Some(x) if x.toLowerCase == "true" => true
        case _ => false
      }
    } else false

    // 如果启用 ES 脚本，获取脚本名和脚本参数
    val esScriptName: String = if (esScriptEnable) {
      confMap.get("script.name") match {
        case Some(x) if x.nonEmpty => x
        case _ => throw new Exception("Found invalid configuration: script.name is nonEmpty which is not allowed")
      }
    } else {
      ""
    }
    val esScriptParam: String = if (esScriptEnable) {
      confMap.get("script.param") match {
        case Some(x) if x.nonEmpty => x
        case _ => throw new Exception("Found invalid configuration: script.name is nonEmpty which is not allowed")
      }
    } else {
      ""
    }

    // 生成新增属性: 统计结果中新增字段：key, keyName, data_type, value_type, cycle
    val STATISTIC_INFO_PREFIX_KEY = "statistic."
    val addFieldsMap = confMap.filter{
      case (k, v) => k.startsWith(STATISTIC_INFO_PREFIX_KEY)
    }.map{case (k, v) => (k.stripPrefix(STATISTIC_INFO_PREFIX_KEY), v)}

    rdd.mapPartitions(iter=>{
      val client: Client = ESUtils.getClient(confMap)

      implicit val formats = DefaultFormats

      new Iterator[String]{
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          val jsonStr = iter.next()

          // json转换为map
          val jValue = parse(jsonStr)
          // 新增属性: 统计结果中新增字段：key, keyName, data_type, value_type, cycle
          val jValue_new1= jValue.merge(render(addFieldsMap))

          val jvalue_map = jValue_new1.extract[Map[String, String]]
          //TODO: 删除测试
//          if (jvalue_map.contains(RAW_DATA_KEY)) {
//            println("= = " * 10 + "[myapp ESOutputRule.output.question] source = " + jvalue_map(RAW_DATA_KEY))
//          }
          val json_map: java.util.Map[String, Object] = new util.HashMap[String, Object]()
          jvalue_map.foreach{case (k, v)=> json_map.put(k, v)}

          val DEFAULT_GROUPBY_NONE_VALUE = "NONE"
          val GROUPBY_NONE_VALUE = confMap.get("groupby.none.key") match {
            case Some(x) if x.nonEmpty => x
            case _ => DEFAULT_GROUPBY_NONE_VALUE
          }

          //md5Id 取值应该有 1 idKeys 统计维度(k+v) start_date+end_date，2 区分人数与人次， value_type 3 data_type,
          //Note: 直接取 esId
          val raw_data_without_value_jValue = jValue_new1.removeField {  // id的获取应该从 esId 去掉 value 属性，否则相同维度但value不同都是插入，增加统计的数据量
            case JField(`valueKey`, _) => true
            case _ => false
          }
          val esId = compact(raw_data_without_value_jValue)
          val md5Id = DigestUtils.md5Hex(esId.getBytes("iso-8859-1"))

          val valueStr = jvalue_map(valueKey)
          // Note：
          val value = valueType.toLowerCase match {
            case "int" | "interger" => valueStr.toInt
            case "long" => valueStr.toLong
            case "double" => valueStr.toDouble
            case _ => valueStr
          }

          // 更新 value 取值(类型)
          json_map.put(valueKey, value.asInstanceOf[Object])
          // 获取更新 raw_data
          val indexRequest = new IndexRequest(idx, typ, md5Id).source(json_map)
          var raw_data = XContentHelper.convertToJson(indexRequest.source(), true)
          json_map.put(RAW_DATA_KEY, raw_data)

          if (esAddUpdateMethod == "getAndUpdate") { // 方式1：先查询再更新
            var getResponse: GetResponse = null
            try{
              getResponse = client.prepareGet(idx, typ, md5Id).execute().actionGet()

              if (getResponse.isExists) { // 存在，update
                val existValueStr = getResponse.getSourceAsMap.get(valueKey).toString

                val newValue = valueType.toLowerCase match {
                  case "int" | "interger" => existValueStr.toInt + valueStr.toInt
                  case "long" => existValueStr.toLong + valueStr.toLong
                  case "double" => existValueStr.toDouble + valueStr.toDouble
                  case _ => existValueStr + valueStr
                }
                // 方式1：Update by merging documents,
                // 更新 value 取值(类型)
                json_map.put(valueKey, newValue.asInstanceOf[Object])
                // 获取更新 raw_data
                json_map.remove(RAW_DATA_KEY)
                val indexRequest2 = new IndexRequest(idx, typ, md5Id).source(json_map)
                raw_data = XContentHelper.convertToJson(indexRequest2.source(), true)
                json_map.put(RAW_DATA_KEY, raw_data)

                val updateRequest = new UpdateRequest(idx, typ, md5Id).doc(jsonBuilder().startObject()
                        .field(valueKey, newValue)
                        .field(RAW_DATA_KEY, raw_data)
                        .endObject())
                client.update(updateRequest).get()

              } else { // 不存在，index
                //logWarning("found document " + idx + "." + typ +"." + md5Id + "does not exists, create it")
                client.prepareIndex(idx, typ, md5Id).setSource(json_map).get()
              }

            } catch {
              case ex: IndexMissingException => //不存在，index
                //logWarning("found index " + idx + "." + typ +"." + md5Id + "does not exists, create it")
                client.prepareIndex(idx, typ, md5Id).setSource(json_map).get()
              case ex: Exception =>
                throw ex
            }
          } else { // 方式2：脚本方式
            val updateRequest =
              // 0.90.11 没有 ScriptType 类
//              (if (esScriptEnable) {
//                new UpdateRequest(idx, typ, md5Id)
//                        .script(esScriptName, ScriptType.FILE).addScriptParam(esScriptParam, value).upsert(indexRequest)
//              } else {
                new UpdateRequest(idx, typ, md5Id)
                        .script(s"ctx._source.$valueKey += p1").addScriptParam("p1", value).upsert(indexRequest)
//              })
            client.update(updateRequest).get()
          }

          //TODO: 删除测试
          // println("= = " * 10 + "[myapp ESOutputRule.output] source = " + raw_data)
          jsonStr
        }
      }
    })
  }
}
