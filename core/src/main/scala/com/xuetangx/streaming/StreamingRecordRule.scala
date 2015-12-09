package com.xuetangx.streaming

/**
 * Created by tsingfu on 15/11/29.
 */
trait StreamingRecordRule extends Serializable {

  /** 外部缓存关联时采用批次查询，在(每个/所有)批次查询获取关联信息后，对批次内的每个数据元素进行处理
    *
    * @param record
    * @return
    */
  def process(record: String,
              key: String,
              //cache_data1: Map[String, Map[String, String]] = null,  // cache_broadcast_value
              //cache_data2: scala.collection.mutable.Map[String, Map[String, String]] = null,  // 批次查询结果累计
              cache_data2: java.util.concurrent.ConcurrentHashMap[String, Map[String, String]] = null,  // 支持并发
              cache_data3: Map[String, Map[String, String]] = null  //每个批次查询结果
                     ): String = {
    record
  }

  /** 外部缓存关联时 cache.query.condition.enabled = true 时，进行判断是否需要外部关联
    *
    * @param record
    * @param key
    * @return
    */
  def queryOrNot(record: Any, key: String): Boolean = {
    true
  }

  var rule_name: String = null

  def setRuleName(rule_name: String): Unit ={
    this.rule_name = rule_name
  }
}
