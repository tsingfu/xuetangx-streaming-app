package com.xuetangx.streaming.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Created by tsingfu on 15/12/5.
 */
object SparkUtils {

  def persist_rdd[T](rdd: RDD[T]): RDD[T] ={
    try {
      rdd.persist()
    } catch {
      case ex: UnsupportedOperationException =>
        null
      case ex => throw ex
    }
  }


  def unpersist_rdd_df(rdd_cache_map: scala.collection.mutable.Map[String, RDD[String]],
                      df_cache_map: scala.collection.mutable.Map[String, DataFrame]): Unit = {
    println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) +
            " [myapp SparkUtils.unpersist_rdd_df] unpersist rdd_cache_map = " +
            rdd_cache_map.keys.mkString("[", ",", "]"))
    rdd_cache_map.foreach{case (k, vrdd) =>
      vrdd.unpersist()
      rdd_cache_map.remove(k)
      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) +
              " [myapp SparkUtils.unpersist_rdd_df] unpersist rdd_cache = " + k)
    }


    println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) +
            " [myapp SparkUtils.unpersist_rdd_df] unpersist rdd_cache_map = " +
            df_cache_map.keys.mkString("[", ",", "]"))
    df_cache_map.foreach{case (k, vdf) =>
      vdf.unpersist()
      df_cache_map.remove(k)
      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) +
              " [myapp SparkUtils.unpersist_rdd_df] unpersist rdd_cache = " + k)
    }
  }
}
