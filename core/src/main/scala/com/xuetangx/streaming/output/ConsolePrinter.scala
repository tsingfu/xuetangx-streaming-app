package com.xuetangx.streaming.output

import com.xuetangx.streaming.StreamingProcessor
import org.apache.spark.rdd.RDD

/**
 * Created by tsingfu on 15/10/10.
 */
class ConsolePrinter extends StreamingProcessor {

  /**输出统计指标到控制台
    *
    * @param rdd
    * @param confMap
    *
    */
  override def output(rdd: RDD[String],
                      confMap: Map[String, String]) = {

    rdd.foreachPartition(iter=>{
      iter.foreach(line=>println("= = " * 10 + "[myapp ConsolePrinter.output]" + line))
    })

  }

}
