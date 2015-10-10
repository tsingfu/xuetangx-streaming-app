package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.StreamingProcessor
import org.apache.spark.rdd.RDD

/**
 * Created by tsingfu on 15/10/9.
 */
class EnhanceOriginRefererProcessor extends StreamingProcessor {

  /**处理过滤和属性增强(取值更新，增减字段等)
    *
    * @param rdd
    * @param confMap
    * @return
    */
  override def process(rdd: RDD[String],
                       confMap: Map[String, String],
                       cacheConfMap: Map[String, String] = null,
                       dataSourceConfMap: Map[String, String] = null): RDD[String] = {

    rdd
  }

}
