package com.xuetangx.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by tsingfu on 15/10/6.
 */
object StreamingReader extends Logging {
  //For StreamingContext
  val BATCH_DURATION_SECONDS_KEY = "batch.duration.seconds"

  //For kafka
  val ZK_CONNECT_KEY = "zookeeper.connect"
  val BROKER_LIST_KEY = "metadata.broker.list"
  val TOPIC_KEY = "topics"
  val GROUP_ID_KEY = "group.id"
  val NUM_CONSUMER_FETCGERS_KEY = "num.consumer.fetchers"
  val NUM_RECEIVERS_KEY = "num.receivers"

  //For Hdfs
  val HDFS_DEFAULT_FS_KEY = "fs.defaultFS"
  val HDFS_PATH_KEY = "path"

  def readSource(ssc: StreamingContext,
                 dataInterfaceConfMap: Map[String, String],
                 appConfMap: Map[String, String]): DStream[String] = {
    val sourceType = dataInterfaceConfMap("source.type")

    val stream = if ("kafka".equals(sourceType)) {
      val kafkaSimpleConsumerApiUsed = appConfMap.getOrElse("kafka.simple.consumer.api.used", "true").toBoolean
      if(kafkaSimpleConsumerApiUsed){
        val topicsSet = dataInterfaceConfMap(StreamingReader.TOPIC_KEY).split(",").toSet
        val brokers = dataInterfaceConfMap(StreamingReader.BROKER_LIST_KEY)
        val kafkaParams = Map[String, String](StreamingReader.BROKER_LIST_KEY -> brokers)
        logInfo("read stream data through Direct approach (No receivers) and Kafka’s simple consumer API " +
                "with config : kafkaParams->" + kafkaParams.mkString("[",",","]") + ", topic->" + topicsSet)
        KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicsSet).map(_._2)

      } else {
        val zkConnect = dataInterfaceConfMap(StreamingReader.ZK_CONNECT_KEY)
        val groupId = appConfMap(StreamingReader.GROUP_ID_KEY)
        val numReceivers = appConfMap(StreamingReader.NUM_RECEIVERS_KEY).toInt
        val numConsumerFetchers = appConfMap(StreamingReader.NUM_CONSUMER_FETCGERS_KEY).toInt

        val topicMap = Map(dataInterfaceConfMap(StreamingReader.TOPIC_KEY) -> numConsumerFetchers)
        logInfo("read stream data through old approach using Receivers and Kafka’s high-level API with config: zookeeper.connect->" + zkConnect + "; group.id->" + groupId + "; topic->" + topicMap)
        val streams = (1 to numReceivers).map(_=>KafkaUtils.createStream(ssc, zkConnect, groupId, topicMap))
        ssc.union(streams).map(_._2)
      }

    } else if ("hdfs".equals(sourceType)) {
      val path = dataInterfaceConfMap(StreamingReader.HDFS_DEFAULT_FS_KEY) + "/" + dataInterfaceConfMap.get("path")
      ssc.textFileStream(path)

    } else {
      throw new Exception("Error: unsupported source.type " + sourceType)

    }

    //是否对数据源进行 repartition
    val stream2 = appConfMap.get("dataInterface.stream.repatition.partitions") match {
      case Some(x) if x.nonEmpty =>
        stream.repartition(x.toInt)
      case _ =>
        stream
    }

    //是否对数据源进行格式化
    val stream3 = dataInterfaceConfMap.get("format.class") match {
      case Some(x) if x.nonEmpty =>
        val formater = Class.forName(x).newInstance().asInstanceOf[StreamingFormater]
        formater.format(stream2)
      case _ => stream2
    }

    stream3
  }
}
