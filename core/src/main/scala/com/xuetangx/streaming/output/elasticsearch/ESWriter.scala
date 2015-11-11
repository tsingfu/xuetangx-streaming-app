package com.xuetangx.streaming.output.elasticsearch

import com.xuetangx.streaming.StreamingProcessor
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.rdd.RDD
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.script.ScriptService.ScriptType
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/12.
 */
class ESWriter extends StreamingProcessor {

  /**输出统计指标到elasticsearch
    *
    * @param rdd 含有统计信息指标的rdd，
    *            格式：统计指标的维度(targetKeys) 统计指标的时间维度(time_key，如每分钟 time_minute_1 ), 统计指标取值value,
    *             需要附加的其他信息：
    * @param confMap 输出接口的配置信息
    */
  override def output(rdd: RDD[String],
                      confMap: Map[String, String]) = {

    val idx = confMap("index")
    val typ = confMap("type")
    //      val idKeys = confMap("statistic.key").split(",") //取值对应targetKeysList中各统计指标维度名，逗号分隔
    val idKeys = confMap("origin.statistic.key").split(",") //取值对应targetKeysList中各统计指标维度名，逗号分隔
    val valueKey = confMap("value.key")
    val valueType = confMap("value.type")

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

    val idKeyDelimiter = confMap.get("id.key.delimiter") match {
      case Some(x) if x.nonEmpty => x
      case _ => "#"
    }

    //TODO: 是否减少job数据
    rdd.foreachPartition(iter=>{
      val client: Client = ESUtils.getClient(confMap)

      val excludingKeys = Set(valueKey)

      implicit val formats = DefaultFormats

      var cnt: Long = 0
      //TODO: 批量提交
      iter.foreach(jsonStr => {

        // json转换为map
        val jValue = parse(jsonStr)
        val jMap = jValue.extract[Map[String, String]]
        //println("= = " * 10 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp ESWriter.output] jMap = " + jMap.mkString("[", ", ", "]") )

        val DEFAULT_GROUPBY_NONE_VALUE = "NONE"
        val GROUPBY_NONE_VALUE = confMap.get("groupby.none.key") match {
          case Some(x) if x.nonEmpty => x
          case _ => DEFAULT_GROUPBY_NONE_VALUE
        }

        //md5Id 取值应该有 1 idKeys 统计维度(k+v) start_date+end_date，2 区分人数与人次， value_type 3 data_type,
        //Note: 直接取 esId
//        val id = idKeys.map(k=>{
//          if (k == GROUPBY_NONE_VALUE) k +":" + GROUPBY_NONE_VALUE else k + ":" + jMap(k)
//        }).mkString(idKeyDelimiter)
//        val md5Id = DigestUtils.md5Hex(id.getBytes("iso-8859-1"))
        //val esId = Utils.strip(compact(jValue \ "raw_data"), "\"")
        val id_jValue = (jValue \ "raw_data").removeField {  // id的获取应该从 esId 去掉 value 属性，否则相同维度但value不同都是插入，增加统计的数据量
          case JField(`valueKey`, _) => true
          case _ => false
        }
        val esId = compact(id_jValue)
        val md5Id = DigestUtils.md5Hex(esId.getBytes("iso-8859-1"))

        val valueStr = jMap(valueKey)
        // Note：
        val value = valueType.toLowerCase match {
          case "int" | "interger" => valueStr.toInt
          case "long" => valueStr.toLong
          case "double" => valueStr.toDouble
          case _ => valueStr
        }


        val source = jsonBuilder().startObject().field(valueKey, value)
        jMap.foreach{case (k, v) =>
          if (!excludingKeys.contains(k)){
            logDebug("= = " * 20 +"[myapp ESWriter.output] source.field = " + k +" => " +v)
            source.field(k, v)
          }
        }
        source.endObject()
        val indexRequest = new IndexRequest(idx, typ, md5Id).source(source)

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
              val updateRequest = new UpdateRequest(idx, typ, md5Id)
                      .doc(jsonBuilder().startObject().field(valueKey, newValue).endObject())
              client.update(updateRequest).get()
            } else { // 不存在，index
              //logWarning("found document " + idx + "." + typ +"." + md5Id + "does not exists, create it")
              client.prepareIndex(idx, typ, md5Id).setSource(source).execute().actionGet()
            }

          } catch {
            case ex: IndexMissingException => //不存在，index
              //logWarning("found index " + idx + "." + typ +"." + md5Id + "does not exists, create it")
              client.prepareIndex(idx, typ, md5Id).setSource(source).execute().actionGet()
            case ex: Exception =>
              throw ex
          }
        } else { // 方式2：脚本方式
          val updateRequest =
            (if (esScriptEnable) {
              new UpdateRequest(idx, typ, md5Id)
                      .script(esScriptName, ScriptType.FILE).addScriptParam(esScriptParam, value)
            } else {
              new UpdateRequest(idx, typ, md5Id)
                      .script(s"ctx._source.$valueKey += p1").addScriptParam("p1", value)
            }).upsert(indexRequest)

          client.update(updateRequest).get()
        }

        logDebug("= = " * 20 + "[myapp ESWriter.output] jsonStr = " + jsonStr +
                ", source = " + XContentHelper.convertToJson(indexRequest.source(), true) +
                ", jMap = " + jMap.mkString("[", ",", "]") +
                ", " + s"ctx._source.$valueKey += $valueStr")
        cnt += 1
      })
      if(cnt > 0) logDebug("= = " * 20 + "[myapp ESWriter.output] cnt = " + cnt)
      // client.close()
    })
  }

  /**定义输出统计指标前的进行的操作，用于附加信息
    *
    * @param rdd
    * @param confMap
    * @return
    */
  override def pre_output(rdd: RDD[String], confMap: Map[String, String]): RDD[String] ={

    val STATISTIC_INFO_PREFIX_KEY = "statistic."

//    val key = confMap("statistic.key")
//    val keyLevel = confMap("statistic.keyLevel")
//    val dataType = confMap("statistic.data_type")
//    val valueType = confMap("statistic.value_type")

    val addFieldsMap = confMap.filter{case (k, v) => k.startsWith(STATISTIC_INFO_PREFIX_KEY)}
            .map{case (k, v) => (k.stripPrefix(STATISTIC_INFO_PREFIX_KEY), v)}
    //val addFieldJsonStr = addFieldsMap.map{case (k, v) => "\"" +k +"\":\"" + v +"\""}.mkString("{", ",", "}")

    //val timeKey = confMap("timeKey")

    rdd.map(jsonStr=>{
      val jValue = parse(jsonStr)

      //添加以 statistic. 开头的属性
      //val jValue_new1 = jValue.merge(parse(addFieldJsonStr))
      val jValue_new1 = jValue.merge(render(addFieldsMap))
      val raw_data = compact(jValue_new1)

      //val jValue_new2 = jValue_new1.merge(parse("{\"raw_data\": \"" +  raw_data.replace("\"", "\\\"") + "\"}"))
      val jValue_new2 = jValue_new1.merge(render(Map[String, JValue]("raw_data" -> raw_data)))

      val res = compact(jValue_new2)
      logDebug("= = " * 20 +"[myapp ESWriter.pre_output] res = " + res)
      res
    })
  }

}
