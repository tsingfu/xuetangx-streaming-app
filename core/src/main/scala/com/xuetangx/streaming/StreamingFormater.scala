package com.xuetangx.streaming

import org.apache.spark.streaming.dstream.DStream

/**
 * Created by tsingfu on 15/11/4.
 */
class StreamingFormater extends Serializable with Logging {

  //TODO: 对流数据进行格式化
  def format(stream: DStream[String]): DStream[String] = {

    // 默认不做格式化，可以自定义格式化插件类，在读取数据流后进行格式化
/*
    stream.transform(rdd => {
      rdd
    })
*/

    stream
  }
}
