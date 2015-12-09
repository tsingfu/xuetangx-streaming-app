package com.xuetangx.streaming.output.elasticsearch

import java.util

import com.xuetangx.streaming.util.Utils
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.{ImmutableSettings, Settings}
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.xcontent.XContentFactory._
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.indices.IndexMissingException
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/12.
 */
object ESUtils {

  // 维护连接池
  // id = serverPort.list + "#" + cluster.name
  val esPool = scala.collection.mutable.Map[String, Client]()

  def getClient(confMap: Map[String, String]): Client = synchronized {
    val esClusterName = confMap("cluster.name")
    val esServerPortList = confMap("serverPort.list")
    getClient(esServerPortList, esClusterName)
  }
  
  def getClient(esServerPortList: String, esClusterName: String): Client = synchronized {
    val id = esServerPortList + "#" + esClusterName

    esPool.getOrElseUpdate(id, {
      val esServerPortStrArr = esServerPortList.split(",").map(_.trim)

      val settings: Settings = ImmutableSettings.settingsBuilder()
              // 指定集群名称
              .put("cluster.name", esClusterName)
              // 探测集群中机器状态
              .put("client.transport.sniff", true).build()

      val transportClient = new TransportClient(settings)

      esServerPortStrArr.foreach(serverPortStr=>{
        val serverPort = serverPortStr.split(":").map(_.trim)
        assert(serverPort.nonEmpty, "found invalid serverPort.list")
        val esHost = serverPort(0)

        val esPort = if (serverPort.length == 2) serverPort(1) else "9300"
        transportClient.addTransportAddress(new InetSocketTransportAddress(esHost, esPort.toInt))
      })

      val client: Client = transportClient
      client
    })
  }

  
  /**
   * 将 json 格式的字符串导入到ES
   *  要求：json格式中含导入ES需要的 es_hosts, index, type, doc_id, body 信息 
   * @param jsonArr
   */
  def importES(jsonArr: Array[String], es_import_conf: Map[String, String]): Unit ={

    val RAW_DATA_KEY = es_import_conf.getOrElse("raw_data.key", "raw_data")
    val esAddUpdateMethod = es_import_conf.getOrElse("add.update.method", "getAndUpdate")
    //TODO: 支持指定多个字段的类型
    //val valueKey = es_import_conf("value.key")
    //val valueType = es_import_conf("value.type")
    val keyTypeMap = es_import_conf.getOrElse("key.type.list", "").trim match {
      case key_type_list if key_type_list.nonEmpty =>
        key_type_list.split(",").map(key_type=>{
          val key_type_arr = key_type.split(":")
          assert(key_type_arr.length == 2, "[myapp] configuration invalid key.type.list")
          //val key = key_type_arr(0)
          //val typ = key_type_arr(1)
          (key_type_arr(0), key_type_arr(1))
        }).toMap
      case _ =>
        Map[String, String]()
    }

    val sum_key_type_map =
      es_import_conf.getOrElse("sum.key.type.list", "").trim match {
        case key_type_list if key_type_list.nonEmpty =>
          key_type_list.split(",").map(key_type => {
            val key_type_arr = key_type.split(":")
            assert(key_type_arr.length == 2, "[myapp] configuration invalid key.type.list")
            //val key = key_type_arr(0)
            //val typ = key_type_arr(1)
            (key_type_arr(0), key_type_arr(1))
          }).toMap
        case _ =>
          Map[String, String]()
      }

    val sum_key_list = sum_key_type_map.keys

    jsonArr.foreach(jsonString =>{
      val jValue = parse(jsonString)
      
      val jValue_import_metadata =  jValue \ "metadata"
      val es_hosts = Utils.strip(compact(jValue_import_metadata \ "es_hosts"), "\"")
      val es_cluster_name = Utils.strip(compact(jValue_import_metadata \ "cluster.name"), "\"")
      val idx = Utils.strip(compact(jValue_import_metadata \ "index"), "\"")
      val typ = Utils.strip(compact(jValue_import_metadata \ "type"), "\"")
      val md5Id = Utils.strip(compact(jValue_import_metadata \ "id"), "\"")

      val jValue_body = jValue \ "body"

      val client: Client = ESUtils.getClient(es_hosts, es_cluster_name)

      implicit val formats = DefaultFormats  // For jValue.extract[Map[String, String]]
      val jvalue_map = jValue_body.extract[Map[String, String]]  //去掉 valueKey 是 id
      //val jvalue_map2 = jValue_body.values
      val json_map: java.util.Map[String, Object] = new util.HashMap[String, Object]()
      jvalue_map.foreach{case (k, v)=> json_map.put(k, v)}

      // 更新 value 取值(类型)
      val sum_key_value_map = sum_key_list.map(key=>{
        val valueStr = jvalue_map(key)
        (key, valueStr)
      })

      keyTypeMap.foreach{case (k, dt)=>
        val valueStr = jvalue_map(k)
        val value = dt.toLowerCase match {
          case "int" | "interger" => valueStr.toInt
          case "long" => valueStr.toLong
          case "double" => valueStr.toDouble
          case _ => valueStr
        }
        json_map.put(k, value.asInstanceOf[Object])
      }

      sum_key_type_map.foreach{case (k, dt)=>
        val valueStr = jvalue_map(k)
        val value = dt.toLowerCase match {
          case "int" | "interger" => valueStr.toInt
          case "long" => valueStr.toLong
          case "double" => valueStr.toDouble
          case _ => valueStr
        }
        json_map.put(k, value.asInstanceOf[Object])
      }

      val raw_data_without_value_jValue = jValue_body removeField  {
        case JField(k, _) => sum_key_type_map.contains(k)
        case _ => false
      }
      val esId = compact(raw_data_without_value_jValue)
      //val md5Id2 = DigestUtils.md5Hex(esId.getBytes("iso-8859-1"))

      // 获取更新 raw_data
      val indexRequest = new IndexRequest(idx, typ, md5Id).source(json_map)
      val raw_data = XContentHelper.convertToJson(indexRequest.source(), true)
      json_map.put(RAW_DATA_KEY, raw_data)

      // 先查询
      if (esAddUpdateMethod == "getAndUpdate") { // 方式1：先查询再更新
      var getResponse: GetResponse = null
        try{
          getResponse = client.prepareGet(idx, typ, md5Id).get()
          if (getResponse.isExists) { // 存在，update

            val json_doc = jsonBuilder().startObject()

            sum_key_value_map.foreach{case (key, old_value)=>
              val value_in_es = getResponse.getSourceAsMap.get(key).toString
              val newValue = sum_key_type_map(key).toLowerCase match {
                case "int" | "interger" => value_in_es.toInt + old_value.toInt
                case "long" => value_in_es.toLong + old_value.toLong
                case "double" => value_in_es.toDouble + old_value.toDouble
                case _ => value_in_es + old_value
              }

              // 方式1：Update by merging documents,
              // 更新 value 取值(类型)
              json_map.put(key, newValue.asInstanceOf[Object])
              json_doc.field(key, newValue)
            }

            // 获取更新 raw_data
            json_map.remove(RAW_DATA_KEY)
            val indexRequest = new IndexRequest(idx, typ, md5Id).source(json_map)
            val raw_data2 = XContentHelper.convertToJson(indexRequest.source(), true)
            json_map.put(RAW_DATA_KEY, raw_data)

            json_doc.field(RAW_DATA_KEY, raw_data2).endObject()

//            val updateRequest = new UpdateRequest(idx, typ, md5Id).doc(jsonBuilder().startObject()
//                    .field(valueKey, newValue)
//                    .field(RAW_DATA_KEY, raw_data2)
//                    .endObject())

            val updateRequest = new UpdateRequest(idx, typ, md5Id).doc(json_doc)
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
//        val updateRequest =
        // 0.90.11 没有 ScriptType 类
        //            (if (esScriptEnable) {
        //              new UpdateRequest(idx, typ, md5Id)
        //                      .script(esScriptName, ScriptType.FILE).addScriptParam(esScriptParam, value).upsert(indexRequest)
        //            } else {
//          new UpdateRequest(idx, typ, md5Id)
//                  .script(s"ctx._source.$valueKey += p1").addScriptParam("p1", value).upsert(indexRequest)
          //            })

        val updateRequest =  new UpdateRequest(idx, typ, md5Id)
        sum_key_value_map.foreach{case (valueKey, value)=>
          updateRequest.script(s"ctx._source.$valueKey += p1").addScriptParam("p1", value)
        }
        updateRequest.upsert(indexRequest)
        client.update(updateRequest).get()
      }
    })
  }
}
