package com.xuetangx.streaming

import org.apache.spark.rdd.RDD

/**
 * Created by tsingfu on 15/11/29.
 */
class StreamingRDDRule extends Serializable with Logging {
  var ruleId: String = null
  var conf: Map[String, String] = null

  // 对于 outputRule， ruleType/ruleMethod/enabled 无效
  var ruleType: String = null
  var ruleMethod: String = null
  var enabled: Boolean = _

  var cacheConf: Map[String, String] = null
  //var cache_broadcast: Broadcast[Map[String, Map[String, String]]] = null

  //TODO: 为外部缓存处理，提供的插件类
  var recordRules: Array[StreamingRecordRule] = null


  var outputConf: Map[String, String] = null
  var outputRule: StreamingRDDRule = null

  def init(ruleId: String, 
           conf: Map[String, String], 
           cacheConf: Map[String, String] = null,
           //cache_broadcast: Broadcast[Map[String, Map[String, String]]] = null,
           outputConf: Map[String, String] = null) = {

    this.ruleId = ruleId
    assert(conf != null, "init with invalid conf!")

    this.conf = conf
    if (conf.nonEmpty) {
      this.ruleType = conf("step.type")
      this.ruleMethod = conf("step.method")
      this.enabled = conf("step.enabled").toBoolean
    } else {
      this.enabled = false
    }

    if (cacheConf != null) {
      this.cacheConf = cacheConf
//      if (cacheConf.getOrElse("broadcast.enabled", "false").toBoolean && cache_broadcast != null) {
//        this.cache_broadcast = cache_broadcast
//      }
    } else {
      this.cacheConf = Map[String, String]()
    }

    val recordRule_clzes = conf.getOrElse("batchProcessor.class.list", "").trim.split(",").map(_.trim).filter(_.nonEmpty)

    recordRules =
      if (recordRule_clzes.nonEmpty) {
        recordRule_clzes.map(clz_name => {
          val record_rule = Class.forName(clz_name).newInstance().asInstanceOf[StreamingRecordRule]
          record_rule.setRuleName(clz_name)
          record_rule
        })
      } else {
        Array[StreamingRecordRule]()
      }

    if (outputConf != null && conf.getOrElse("output.dataInterfaceId", "").nonEmpty) {
      this.outputConf = outputConf
    } else {
      this.outputConf = Map[String, String]()
    }

    val clz_name = this.conf.getOrElse("output.class", "com.xuetangx.streaming.rules.ConsoleOutputRule").trim

    if (clz_name != "com.xuetangx.streaming.rules.ConsoleOutputRule") {
      this.outputRule = Class.forName(clz_name).newInstance().asInstanceOf[StreamingRDDRule]
      val output_rule_id = this.ruleId + "_output_" + this.conf("output.dataInterfaceId")
      outputRule.setRuleId(output_rule_id)
      outputRule.setOutputConf(this.outputConf)
    }
  }

  def setRuleId(ruleId: String): Unit = {
    this.ruleId = ruleId
  }

  def setConf(conf: Map[String, String]): Unit = {
    if (conf.nonEmpty) {
      this.ruleType = conf("step.type")
      this.ruleMethod = conf("step.method")
      this.enabled = conf("step.enabled").toBoolean
    } else {
      this.enabled = false
    }
  }

  def setCacheConf(cacheConf: Map[String, String]): Unit = {
    this.cacheConf = cacheConf
  }

//  def setCacheBroadcast(cache_broadcast: Broadcast[Map[String, Map[String, String]]]): Unit = {
//    if (cache_broadcast != null) {
//      this.cache_broadcast = cache_broadcast
//    }
//  }

  def setOutputConf(outputConf: Map[String, String]): Unit = {
    this.outputConf = outputConf
  }

  def setOutputRule(outputRule: StreamingRDDRule): Unit = {
    this.outputRule = outputRule
  }

  /**格式化
    *
    * @param rdd
    * @return
    */
  def format(rdd: RDD[String]): RDD[String] = {
    rdd
  }

  /**处理过滤和属性增强(取值更新，增减字段等)
    *
    * @param rdd
    * @return
    */
  def process(rdd: RDD[String]
              //,
              //cache_broadcast: Broadcast[Map[String, Map[String, String]]] = null,
              //fun_get_broadcast_value: (String) => Map[String, Map[String, String]]
              //fun_get_broadcast_value: () => Broadcast[Map[String, Map[String, Map[String, String]]]]
                     ): RDD[String] = {
    rdd
  }

  /**计算统计指标，使用插件类实现计算统计指标时，需要重写自定义
    *
    * @param rdd
    * @return
    */
  def compute(rdd: RDD[String]): RDD[String] = {
    rdd
  }


  /**输出统计指标
    *
    * @param rdd
    */
  def output(rdd: RDD[String]): RDD[String] = {
    rdd
  }
}