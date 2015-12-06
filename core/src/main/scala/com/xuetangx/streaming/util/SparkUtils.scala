package com.xuetangx.streaming.util

import org.apache.spark.rdd.RDD

/**
 * Created by tsingfu on 15/12/5.
 */
object SparkUtils {

  def persist_rdd[T](rdd: RDD[T]): Unit ={
    try {
      rdd.persist()
    } catch {
      case ex: UnsupportedOperationException =>
      case ex => throw ex
    }
  }
}
