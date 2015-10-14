package com.xuetangx.streaming

import com.xuetangx.streaming.util.Utils

import scala.xml.XML

/**
 * Created by tsingfu on 15/10/14.
 */
class AppConf extends Serializable {

  var appsPropMap: Map[String, Map[String, String]] = _
  var interfaceId: String = _
//  var dataSourcesPropMap: Map[String, Map[String, String]] = _
  var inputInterfacePropMap: Map[String, String] = _
  var outputInterfacesPropMap: Map[String, Map[String, String]] = _
  var cachesPropMap: Map[String, Map[String, String]] = _
  var preparesActivePropSeq: Seq[(String, Map[String, String])] = _
  var computesActiveConfTupleSeq: Seq[(String, Seq[(String, Map[String, String])])] = _

  def init(confFileXml: String, appId: String): Unit ={
    // 解析配置
    val conf = XML.load(confFileXml)
    /*
        val (appsCommonPropMap, appsPropMap) = parseProperties2Map(conf\ "apps", "app", "id")
        val (dataSourcesCommonPropMap, dataSourcesPropMap) = parseProperties2Map(conf \ "dataSources", "source", "id")
        val (dataInterfaceCommonPropMap, dataInterfacesPropMap) = parseProperties2Map(conf \ "dataInterfaces", "interface", "id")
        val (cacheCommonPropMap, cachesPropMap) = parseProperties2Map(conf\ "externalCaches", "cache", "id")
    */

    //    val monitor = Class.forName("com.xuetangx.streaming.monitor.MConsolePrinter").newInstance()

    // 应用的配置
    appsPropMap = Utils.parseProperties2Map(conf\ "apps", "app", "app.id")
    interfaceId = appsPropMap(appId)("app.interfaceId") //获取输入接口id

    // 数据源的配置
    val dataSourcesPropMap = Utils.parseProperties2Map(conf \ "dataSources", "source", "source.id")

    //TODO: 更新
    // 数据接口的配置，分2类： input, output
    val dataInterfacesPropMap = Utils.parseProperties2Map(conf \ "dataInterfaces", "interface", "interface.id")
    // 输出接口的配置
    val outputDataInterfacesPropMap = dataInterfacesPropMap.filter{case (id, mapConf) => mapConf("interface.type") == "output"}
    outputInterfacesPropMap = outputDataInterfacesPropMap.map{case (id, mapConf) =>
      (id, mapConf ++ dataSourcesPropMap(dataInterfacesPropMap(id)("interface.sourceId")))
    }

    // 外部缓存的配置
    cachesPropMap = Utils.parseProperties2Map(conf\ "externalCaches", "cache", "cache.id")

    // 指定的输入接口配置（含数据源信息）
    inputInterfacePropMap = dataInterfacesPropMap(interfaceId) ++
            dataSourcesPropMap(dataInterfacesPropMap(interfaceId)("interface.sourceId"))

    // 准备阶段配置
    val preparesConf = (conf \ "prepares").filter(_.attribute("interfaceId").get.text==interfaceId)
    //    val (preparesCommonPropMap, preparesPropMap) = parseProperties2Map(preparesConf, "prepare", "id") //报错
    val preparesPropMap = Utils.parseProperties2Map(preparesConf, "step", "step.id")

    // 指定的计算阶段配置
    val computeStatisticsConf = (conf \ "computeStatistics").filter(node=>{
      //      node.attribute("interfaceId")==interfaceId
      node.attribute("interfaceId").get.text==interfaceId
    })

    // 指定数据接口id计算统计指标的计算配置(计算统计指标的数据集id->是否启用->指定id的配置)
    val computesConfTuple = for (computeStatisticConf <- computeStatisticsConf \ "computeStatistic") yield {
      val outerAttrMap = computeStatisticConf.attributes.asAttrMap
      //      (outerAttrMap, parseProperties2Map(computeStatisticConf, "step", "step.id"))
      val res = Utils.parseProperties2Map(computeStatisticConf, "step", "step.id").map{case (k, vMap)=>
        (k, vMap ++ outerAttrMap.map{case (outKey, v) => (computeStatisticConf.head.label+"."+outKey, v)})
      }
      println("=  = " * 20)
      println(outerAttrMap.mkString("[", ",", "]"))
      (outerAttrMap("id"), outerAttrMap("enabled"), res)
    }

    // 准备阶段配置中有效步骤的配置
    preparesActivePropSeq = preparesPropMap.filter{case (k, v)=>
      v.getOrElse("prepare.enabled", "false") == "true"
    }.toSeq.sortWith(_._1 < _._1)

    // 指定数据接口id有效的计算配置
    computesActiveConfTupleSeq = computesConfTuple.filter(_._2=="true").map(kxv=>{
      val (key, flag, outerMap) = kxv

      val filteredStepsMap = outerMap.filter{case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"}
      (key, filteredStepsMap.toSeq.sortWith(_._1 < _._1))
    }).sortWith(_._1 < _._1)
  }
}
