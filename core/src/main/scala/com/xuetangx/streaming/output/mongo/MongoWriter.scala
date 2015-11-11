package com.xuetangx.streaming.output.mongo

import com.xuetangx.streaming.StreamingProcessor
import org.apache.spark.rdd.RDD

/**
 * Created by tsingfu on 15/10/10.
 */
class MongoWriter extends StreamingProcessor {

  /**输出统计指标到mongodb
    * 需要重写
    *
    * @param rdd
    * @param confMap
    */
  override def output(rdd: RDD[String],
                      confMap: Map[String, String]) = {

    rdd
  }

}
