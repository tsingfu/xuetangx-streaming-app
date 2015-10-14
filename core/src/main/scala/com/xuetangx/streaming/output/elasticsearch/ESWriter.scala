package com.xuetangx.streaming.output.elasticsearch

import com.xuetangx.streaming.StreamingProcessor
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.common.xcontent.XContentHelper
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/12.
 */
class ESWriter extends StreamingProcessor {

  /**输出统计指标到elasticsearch
    *
    * @param rdd 含有统计信息指标的rdd，格式：time_key, other_keysList, value, other_info(doc_type)
    * @param confMap 输出接口的配置信息
    */
  override def output(rdd: RDD[String],
                      confMap: Map[String, String]) = {

    rdd.foreachPartition(iter=>{
      val client: Client = ESUtils.getClient(confMap)
      
      val idx = confMap("index")
      val typ = confMap("type")
//      val idKey = confMap("id.key") //Note：取统计维度的所有属性值
      val idKeys = confMap("id.keyNames").split(",") //取值对应targetKeysList中各统计指标维度名，逗号分隔
      val valueKey = confMap("value.key")
      val valueType = confMap("value.type")
      val excludingKeys = Set(valueKey)

      val idKeyDelimiter = confMap.get("id.key.delimiter") match {
        case Some(x) if x.nonEmpty => x
        case _ => "-"
      }
      implicit val formats = DefaultFormats

      var cnt: Long = 0
      var sum: Long = 0
      //TODO: 批量提交
      iter.foreach(jsonStr => {

        // json转换为map
        val jValue = parse(jsonStr)

        val jMap = jValue.extract[Map[String, String]]

        val id = idKeys.map(k=>jMap(k)).mkString(idKeyDelimiter)
//        val id = jMap(idKey)
        val valueStr = jMap(valueKey)
        // Note：
        val value = valueType.toLowerCase match {
          case "int" | "interger" => valueStr.toInt
          case "long" => valueStr.toLong
          case "double" => valueStr.toDouble
          case _ => valueStr
        }

        val source = jsonBuilder().startObject().field(valueKey, value)
        jMap.foreach{case (k, v)=> 
          if (!excludingKeys.contains(k))
            source.field(k, v)
        }
        source.endObject()
        
        val indexRequest = new IndexRequest(idx, typ, id)
                .source(source)
        val updateRequest = new UpdateRequest(idx, typ, id)
                .script(s"ctx._source.$valueKey += p1").addScriptParam("p1", value)
                .upsert(indexRequest)

        client.update(updateRequest).get()

        // TODO: 删除测试信息
        println("= = " * 20 + "[myapp ESWriter.output] jsonStr = " + jsonStr +
                ", source = " + XContentHelper.convertToJson(indexRequest.source(), true) +
                ", jMap = " + jMap.mkString("[", ",", "]") +
                ", " + s"ctx._source.$valueKey += $valueStr")
        cnt += 1
        sum += valueStr.toLong
      })
      if(cnt > 0) println("= = " * 20 + "[myapp ESWriter.output] cnt = " + cnt + ", sum = " + sum)
      client.close()
    })
  }


}
