package com.xuetangx.streaming

import org.apache.spark.rdd.RDD

/**
 * Created by tsingfu on 15/10/8.
 */
class StreamingProcessor {

  /**处理过滤和属性增强(取值更新，增减字段等)
   *
   * @param rdd
   * @param confMap
   * @return
   */
  def process(rdd: RDD[String],
              confMap: Map[String, String],
              cacheConfMap: Map[String, String] = null,
              dataSourceConfMap: Map[String, String] = null): RDD[String] = {
    rdd
  }

  /**计算统计指标
   *
   * @param rdd
   * @param confMap
   * @return
   */
  def compute(rdd: RDD[String], confMap: Map[String, String]): RDD[String] = {
    rdd
  }


  /**输出统计指标
   *
   * @param rdd
   * @param confMap
   */
  def output(rdd: RDD[String], confMap: Map[String, String]) = {

  }

}
