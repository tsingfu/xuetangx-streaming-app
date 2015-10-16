package com.xuetangx.streaming.common

/**
 * Created by tsingfu on 15/10/14.
 */
class InBatchProcessor extends Serializable {

  /** 外部缓存关联时采用批次查询，在(每个/所有)批次查询获取关联信息后，对批次内的每个数据元素进行处理
   * 
   * @param record
   * @return
   */
  def process(record: String,
              key: String,
              cacheData: Map[String, Map[String, String]]): String = {
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
}
