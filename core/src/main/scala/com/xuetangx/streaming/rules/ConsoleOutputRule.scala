package com.xuetangx.streaming.rules

import com.xuetangx.streaming.StreamingRDDRule
import org.apache.spark.rdd.RDD

/**
 * Created by tsingfu on 15/12/1.
 */
class ConsoleOutputRule extends StreamingRDDRule {

  /**输出统计指标到控制台
    *
    * @param rdd
    *
    */
  override def output(rdd: RDD[String]) = {

    /*
        rdd.foreachPartition(iter=>{
          iter.foreach(line=>println("= = " * 10 + "[myapp ConsoleOutputRule.output]" + line))
        })
    */

    rdd.mapPartitions(iter=>{
      new Iterator[String] {
        override def hasNext: Boolean = iter.hasNext

        override def next(): String = {
          val line = iter.next()
          println("= = " * 10 + "[myapp ConsoleOutputRule.output]" + line)
          line
        }
      }
    })

  }

}
