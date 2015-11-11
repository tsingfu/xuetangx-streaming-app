package com.xuetangx.streaming

import com.xuetangx.streaming.util.{DateFormatUtils, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.functions._

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.JsonAST.JField
import org.json4s.jackson.JsonMethods._

import scala.collection.mutable
import scala.xml.XML

object StreamingApp {

  def main(args: Array[String]): Unit ={
    if (args.length != 4) {
      System.err.println(
        """Error: found invalid parameters
          |usage:  ./bin/spark-class com.xuetangx.streaming.StreamingApp <master> <appName> <confFile.xml> <appId>
        """.stripMargin)
      System.exit(1)
    }
    val Array(master, appName, confFileXml, appId) = args


    // 解析配置
    val conf = XML.load(confFileXml)

//    val monitor = Class.forName("com.xuetangx.streaming.monitor.MConsolePrinter").newInstance()

    // 应用的配置
    val appsPropMap = Utils.parseProperties2Map(conf \ "apps", "app", "app.id")
    val interfaceId = appsPropMap(appId)("app.interfaceId") //获取输入接口id

    // 数据源的配置
    val dataSourcesPropMap = Utils.parseProperties2Map(conf \ "dataSources", "source", "source.id")

    // 数据接口的配置，分2类： input, output
    val dataInterfacesPropMap = Utils.parseProperties2Map(conf \ "dataInterfaces", "interface", "interface.id")
    // 输出接口的配置
    val dataInterfacesPropMap_output = dataInterfacesPropMap.filter{case (id, confMap) => confMap("interface.type") == "output"}
    val outputInterfacesPropMap = dataInterfacesPropMap_output.map{case (id, confMap) =>
      (id, confMap ++ dataSourcesPropMap(dataInterfacesPropMap(id)("interface.sourceId")))
    }

    // 外部缓存的配置
//    val cachesPropMap = Utils.parseProperties2Map(conf\ "externalCaches", "cache", "cache.id")
    val cachesPropMap = Utils.parseProperties2Map(conf\ "externalCaches", "cache", "cache.id").map{case (cacheId, confMap)=>
      (cacheId, confMap ++ dataSourcesPropMap(confMap("cache.sourceId")))
    }

    // 指定的输入接口配置（含数据源信息）
    val inputInterfacePropMap = dataInterfacesPropMap(interfaceId) ++
            dataSourcesPropMap(dataInterfacesPropMap(interfaceId)("interface.sourceId"))

    // 指定的输入数据接口关联的准备阶段配置
    val preparesConf = (conf \ "prepares").filter(_.attribute("interfaceId").get.text==interfaceId)
    //    val (preparesCommonPropMap, preparesPropMap) = parseProperties2Map(preparesConf, "prepare", "id") //报错
    val preparesPropMap = Utils.parseProperties2Map(preparesConf, "step", "step.id")

    // 指定的计算阶段配置
    val computeStatisticsConf = (conf \ "computeStatistics").filter(node=>{
      // node.attribute("interfaceId")==interfaceId
      node.attribute("interfaceId").get.text==interfaceId
    })

    // 指定数据接口id计算统计指标的计算配置(计算统计指标的数据集id->是否启用->指定id的配置)
    //   Seq[(computeStatistic.id, computeStatistic配置)], 其中 computeStatistic 配置=> Seq(step.id-> step配置) ，其中 step 配置=> Map[k, v]
    val computesConfTuple = for (computeStatisticConf <- computeStatisticsConf \ "computeStatistic") yield {
      val outerAttrMap = computeStatisticConf.attributes.asAttrMap
      //      (outerAttrMap, parseProperties2Map(computeStatisticConf, "step", "step.id"))
      val res = Utils.parseProperties2Map(computeStatisticConf, "step", "step.id").map{case (k, vMap)=>
        (k, vMap ++ outerAttrMap.map{case (outKey, v) => (computeStatisticConf.head.label+"."+outKey, v)})
      }
      println("=  = " * 20)
      println(outerAttrMap.mkString("[", ",", "]"))
      (outerAttrMap("id"), outerAttrMap("enabled"), res)
    }

    // 准备阶段配置中有效步骤的配置
    val preparesPropSeq_active = preparesPropMap.filter{case (k, v)=>
      //v.getOrElse("prepare.enabled", "false") == "true"
      v.getOrElse("step.enabled", "false") == "true"
    }.toSeq.sortWith(_._1 < _._1)

    // 指定数据接口id有效的计算配置
    // 将计算阶段配置中准备和计算区分开
    // compute_preparesConfTuple2: Seq[(computeStatistic.id, computeStatistic.enabled, compute_preparesConfMap)]
    //  其中 compute_preparesConfMap： Map[step.id, Map[String, String]]]
    val compute_preparesConfTuple = for (computeStatisticConf <- computeStatisticsConf \ "computeStatistic") yield {
      val outerAttrMap = computeStatisticConf.attributes.asAttrMap  // Map(computeStatistic.id->x, computeStatistic.enabled->y)
      //      (outerAttrMap, parseProperties2Map(computeStatisticConf, "step", "step.id"))
      val compute_preparesConfMap = Utils.parseProperties2Map(computeStatisticConf \ "prepares", "step", "step.id").map{case (k, vMap)=>
        (k, vMap ++ outerAttrMap.map{case (outKey, v) => (computeStatisticConf.head.label+"."+outKey, v)})
      }

      println("=  = " * 20)
      println(outerAttrMap.mkString("[", ",", "]"))
      (outerAttrMap("id"), outerAttrMap("enabled"), compute_preparesConfMap)
    }

    // compute_computesConfTuple2: Seq[(computeStatistic.id, computeStatistic.enabled, computesConfMap)]
    //  其中 computesConfMap： Map[step.id, Map[String, String]
    val compute_computesConfTuple = for (computeStatisticConf <- computeStatisticsConf \ "computeStatistic") yield {
      val outerAttrMap = computeStatisticConf.attributes.asAttrMap

      val compute_computesConfMap = Utils.parseProperties2Map(computeStatisticConf \ "computes", "step", "step.id").map{case (k, vMap)=>
        (k, vMap ++ outerAttrMap.map{case (outKey, v) => (computeStatisticConf.head.label+"."+outKey, v)})
      }

      println("=  = " * 20)
      println(outerAttrMap.mkString("[", ",", "]"))

      // (computeStatistic.id, computeStatistic.enabled, computesConfMap)
      (outerAttrMap("id"), outerAttrMap("enabled"), compute_computesConfMap)
    }

    // 指定数据接口id有效的计算配置
    val compute_preparesConfMap_active = compute_preparesConfTuple.filter(_._2=="true").map(kxv=>{
      val (computeStatisticId, flag, stepConfMap) = kxv

      val filteredStepsMap = stepConfMap.filter{case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"}
      (computeStatisticId, filteredStepsMap.toSeq.sortWith(_._1 < _._1))
    }).toMap
    // .sortWith(_._1 < _._1)

    val compute_computesConfMap_active = compute_computesConfTuple.filter(_._2=="true").map(kxv=>{
      val (computeStatisticId, flag, stepConfMap) = kxv

      val filteredStepsMap = stepConfMap.filter{case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"}
      (computeStatisticId, filteredStepsMap.toSeq.sortWith(_._1 < _._1))
    }).toMap
    // .sortWith(_._1 < _._1)


    // 初始化
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)

    //Note: spark-streaming 调优设置
    appsPropMap(appId).get("spark.streaming.kafka.maxRatePerPartition") match {
      case Some(x) if x.nonEmpty => sparkConf.set("spark.streaming.kafka.maxRatePerPartition", x.trim)
      case _ =>
    }

    appsPropMap(appId).get("spark.streaming.blockInterval") match {
      case Some(x) if x.nonEmpty => sparkConf.set("spark.streaming.blockInterval", x.trim)
      case _ =>
    }

    appsPropMap(appId).get("spark.streaming.receiver.maxRate") match {
      case Some(x) if x.nonEmpty => sparkConf.set("spark.streaming.receiver.maxRate", x.trim)
      case _ =>
    }

    appsPropMap(appId).get("spark.streaming.concurrentJobs") match {
      case Some(x) if x.nonEmpty => sparkConf.set("spark.streaming.concurrentJobs", x.trim)
      case _ =>
    }




    val sc = new SparkContext(sparkConf)
    appsPropMap(appId).get("checkpointDir") match {
      case Some(x) if x.nonEmpty => sc.setCheckpointDir(x)
      case _ =>
    }

    //TODO: 校验配置
//    val appConf = new AppConf()
//    appConf.init(confFileXml, appId)

    val batchDurationSeconds = appsPropMap(appId)("batchDuration.seconds").toInt
    val ssc = new StreamingContext(sc, Seconds(batchDurationSeconds))
    // TODO: spark-streaming 调优设置
/*
    // 问题： java.io.NotSerializableException: DStream checkpointing has been enabled but the DStreams with their functions are not serializable \n queueStream doesn't support checkpointing
    appsPropMap(appId).get("checkpointDir") match {
      case Some(x) if x.nonEmpty => ssc.checkpoint(x)
      case _ =>
    }
*/


    val sqlc = new SQLContext(sc)

    // spark-sql 调优设置
    appsPropMap(appId).get("spark.sql.shuffle.partitions") match {
      case Some(x) if x.nonEmpty => sqlc.setConf("spark.sql.shuffle.partitions", x)
      case _ =>
    }

    // 读取数据
    val stream = StreamingReader.readSource(ssc, inputInterfacePropMap, appsPropMap(appId))

    /*
        //测试idea连接本地正常，但连接虚拟主机失败，提示org.apache.spark.SparkException: Couldn't find leader offsets for Set([topic1-platformlog,1], [topic2-vpclog,2], [topic1-platformlog,0], [topic2-vpclog,0], [topic2-vpclog,1], [topic1-platformlog,2])
        stream.foreachRDD(rdd=>{
          rdd.foreach(println)
        })
    */

    // 流处理
    //  初始化处理指定数据接口的类
    val clz = appsPropMap(appId).getOrElse("class", "com.xuetangx.streaming.StreamingApp")
    val instance = Class.forName(clz).newInstance().asInstanceOf[StreamingApp]
    //  流数据处理流程
    instance.process(stream, ssc, sqlc, 
      inputInterfacePropMap, outputInterfacesPropMap,
      cachesPropMap,
      preparesPropSeq_active,
      //computesConfTupleSeq_active,
      compute_preparesConfMap_active, compute_computesConfMap_active
    )

    ssc.start()
    ssc.awaitTermination()
  }


}


/** 20151009 实现批次过滤
 *
 */
class StreamingApp extends Serializable with Logging {

  /** 定义流数据处理流程
    * 1 文本日志到DataFrame的转换，如果日志是json字符串，直接转换(可优化的点1，可配置schema)；如果csv等，配置字段名先转换为json形式，再处理
    * 2
    * 3
    * @param dStream
    */
  def process(dStream: DStream[String],
              ssc: StreamingContext, //For 最近上一批次和本批次排重
              sqlc: SQLContext,
              inputInterfaceConfMap: Map[String, String],
              outputInterfacesPropMap: Map[String, Map[String, String]],
              cachesPropMap: Map[String, Map[String, String]],
              preparesPropSeq_active: Seq[(String, Map[String, String])],
              compute_preparesConfMap_active: Map[String, Seq[(String, Map[String, String])]],
              compute_computesConfMap_active: Map[String, Seq[(String, Map[String, String])]]): Unit = {

    // 初始化批次排重数据结构
    val batchDeduplicateQueueRDDsMap = mutable.Map[String, mutable.Queue[RDD[String]]]()

    // Seq[(computeStatistic.id, computeStatistic配置)] 进行指标统计的
    // 其中 computeStatistic 配置=> Seq(step.id-> step配置) ，其中 step 配置=> Map[k, v]
    // batchDeduplicateConfSeq = Seq[(computeStatistic.id, 是否进行相邻批次排重)]
    val batchDeduplicateConfSeq = compute_preparesConfMap_active.map{case (computeStatisticId, stepConfSeq) =>
      val hasBatchDeduplicateStep =  //指定 computeStatisticId 是否含有 true
        stepConfSeq.map { case (stepId, stepConf) =>
          if (stepConf("step.type") == "batchDeduplicate") "true" else "false"
        }.contains("true")
      (computeStatisticId, hasBatchDeduplicateStep)
    }.filter(x => x._2)

    batchDeduplicateConfSeq.map(_._1).foreach(id=>{
      batchDeduplicateQueueRDDsMap.put(id, new mutable.Queue[RDD[String]]())
    })

    // 准备阶段处理
    // Note: 准备阶段不支持 batchDeduplicate 步骤
    //  准备阶段处理1
    val dStream2 =
      if (inputInterfaceConfMap("type") == "json") dStream
      else {
        // 约定后续处理要求数据流格式为 json 形式字符串，必须在配置文件中显示配置确认
        throw new Exception("Error: format.class of configuration in dataInterfaces is subclass of StreamingFormater, should return format of json string!!!")
      }

    // Note: dStream_after_prepares 中的 rdd 是json形式
    val dStream_after_prepares =
      if (preparesPropSeq_active.isEmpty) { //准备阶段没有配置步骤
        dStream2
      } else { //准备阶段配置了执行步骤

        dStream2.transform(rddJson => {
          val df = sqlc.jsonRDD(rddJson)
          val schema = df.schema

          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] df.schema.length = " + df.schema.length)
          // MConsolePrinter.output("df.schema.length = " + df.schema.length, "StreamingApp.process", "= = " * 8)

          val rdd2 =
            if (schema.fieldNames.nonEmpty) {
              //没有数据时快速跳过job执行
              println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] df.schema.fieldNames.nonEmpty = true ")
              //Note: 准备，分多个步骤
              // 分2类，过滤，增强；
              //  过滤和增强都有2种方式：spark-sql, plugin

              // 初始化准备阶段每个步骤输出结果
              var res_any: Any = df
              var res_schema: StructType = schema

              // 遍历准备阶段准备步骤
              preparesPropSeq_active.foreach {
                case (id, confMap) =>
                  val phaseStep = "prepares.id[" + confMap("prepares.id") +"]-prepare.id[" + id +"]"
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing prepares" + preparesPropSeq_active.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] res_any is RDD or not = " + res_any.isInstanceOf[RDD[String]] +", is DataFrame or not = " + res_any.isInstanceOf[DataFrame] + ", prepares.step.id = " + id)

                  val res = processStep(res_any, res_schema, sqlc,
                    confMap, cachesPropMap, outputInterfacesPropMap,
                    phaseStep)
                  res_any = res._1
                  res_schema = res._2
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] finish processing prepares" + preparesPropSeq_active.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep)
              }

              res_any match {
                case x: RDD[String] => x
                case x: DataFrame => x.toJSON
              }
            } else {
              rddJson
            }
          rdd2
        })
      }

    // 计算统计指标阶段处理
    //Note: 循环流程： 准备(过滤->增强)->批次排重+计算->输出
    //  过滤2(依赖增强1的属性)
    //  增强2(在过滤2的基础上)
//    if(computesActiveConfTupleSeq.isEmpty) { //配置中没有配置有效的计算，不需要触发job
    if(compute_preparesConfMap_active.isEmpty && compute_computesConfMap_active.isEmpty) { //配置中没有配置有效的计算，不需要触发job
/*
      dStream_after_prepares.foreachRDD(rddJson => {
        logWarning("[myapp configuration] found no active computeStatistic in computeStatistics part")
        // rddJson.filter(line => false).count()
        rddJson.filter(line => false)
      })
*/
    } else { //存在有效的计算配置
      //TODO: 测试内存缓存
      dStream_after_prepares.persist()

      // 每个有效的 computeStatistic 对应一个过滤条件
//      val id2streams = computesActiveConfTupleSeq.map {
      val id2streams =
        compute_preparesConfMap_active.map { case (computeStatisticId, stepsConfSeq) =>
          //Note: 每个 computeStatisticId 对应一些过滤条件，
          //   应该有各自的 res_any_in_computeStatistic, res_schema_in_computeStatistic, 初始化可以是准备阶段的值,
          //   但是问题是定义时不在 DStream.transform中执行，有问题，所以初始化为null
          // 输入：dStream_after_prepares
          // 输出：

          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing computeStatistics " + stepsConfSeq.map(_._1).mkString("[", ",", "]") + " in computeStatisticId " + computeStatisticId)

          // 处理计算阶段流程，准备步骤 - 1先处理filter 和 enhance
//          val computePreparesStepsConfSeq = stepsConfSeq.filterNot {
//            case (id, confMap) => confMap("step.type") == "compute" || confMap("step.type") == "batchDeduplicate"
//          }
          val computePreparesStepsConfSeq = stepsConfSeq.filter{case (stepId, stepConfMap) => stepConfMap("step.type") != "batchDeduplicate"}

          // 计算每个有效统计指标集合——先执行准备步骤，再计算，最后输出
          val dStream3 =
            dStream_after_prepares.transform(rddJson => {
              var res_any_in_computeStatistic: Any = rddJson
              var res_schema_in_computeStatistic: StructType = null

              // 遍历计算阶段准备步骤
              computePreparesStepsConfSeq.foreach {
                case (id, confMap) =>
                  val phaseStep = "computeStatistic.id[" + computeStatisticId + "]-prepares.step.id[" + id + "]"
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))

                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] res_any is RDD or not = " + res_any_in_computeStatistic.isInstanceOf[RDD[String]] + ", phaseStep " + phaseStep)
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] res_any is DataFrame or not = " + res_any_in_computeStatistic.isInstanceOf[DataFrame] + ", phaseStep " + phaseStep)

                  val res = processStep(res_any_in_computeStatistic, res_schema_in_computeStatistic, sqlc,
                    confMap, cachesPropMap, outputInterfacesPropMap,
                    phaseStep)
                  res_any_in_computeStatistic = res._1
                  res_schema_in_computeStatistic = res._2

                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process finish processing phaseStep " + phaseStep)
              }

              res_any_in_computeStatistic match {
                case x: RDD[String] => x
                case x: DataFrame => x.toJSON
              }
          })
          (computeStatisticId, dStream3)
      }

      // 处理计算阶段流程，计算步骤 - 3 批次去重 + 计算统计指标
      id2streams.foreach {
      //id2streams.par.foreach {
        case (computeStatisticId, stream) =>

          // val computesStepConfSeq = computesActiveConfTupleSeq.toMap.get(computeStatisticId).get.filter { case (id, confMap) => confMap("step.type") == "compute" }
          // val batchDeduplicateStepConfSeq = computesActiveConfTupleSeq.toMap.get(computeStatisticId).get.filter { case (id, confMap) => confMap("step.type") == "batchDeduplicate" }
          val computesStepConfSeq = compute_computesConfMap_active.get(computeStatisticId).get
          val batchDeduplicateStepConfSeq = compute_preparesConfMap_active.get(computeStatisticId).get.filter { case (id, confMap) => confMap("step.type") == "batchDeduplicate" }

          if (computesStepConfSeq.isEmpty) { //配置中没有配置有效的计算统计指标，不需要action操作，触发job
            stream.foreachRDD(rddJson => {
              println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [Warning myapp configuration] StreamingApp.process.compute found no active computeStatistic.computes in computeStatistics part")
              //rddJson
            })
          } else { //存在有效的计算配置
            if(batchDeduplicateStepConfSeq.isEmpty) { // 没有配置批次排重，直接执行计算统计指标步骤
              //TODO: 测试内存缓存
              //stream.persist()

              // 多少个 computeStatistic，多少个stream，多少个job
              // stream： DStream[String]
              stream.foreachRDD(rddJson => {
                rddJson.persist()  // 问题： 不能与 stream.persist() 一同使用，否则报错：java.lang.UnsupportedOperationException: Cannot change storage level of an RDD after it was already assigned a level

                val df = sqlc.jsonRDD(rddJson)
                val schema = df.schema
                if (schema.fieldNames.isEmpty) {
                  //数据为空
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes found empty dataframe after computePrepare in computeStatisticId = " + computeStatisticId)
                } else {
                  //数据不为空
                  val compute_step_rdds =
                    computesStepConfSeq.map {
                    //computesStepConfSeq.par.foreach {
                      case (id, confMap) =>
                        val phaseStep = "computeStatistic.id[" + computeStatisticId + "]-computes.step.id[" + id + "]"
                        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))

                        // compute 步骤输出的结果是 每个 computeStatistic.computes的结果
                        val res = processStep(df, schema, sqlc,
                          confMap, cachesPropMap, outputInterfacesPropMap,
                          phaseStep)

                        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process finish processing phaseStep " + phaseStep)
                        res._1.asInstanceOf[RDD[String]]
                    }

                  val compute_step_rdds_nonEmpty = compute_step_rdds.filter(_.partitions.nonEmpty) // 不使用 rdd.isEmpty 的原因，避免触发job
                  if (compute_step_rdds_nonEmpty.nonEmpty) {
                    val result = compute_step_rdds_nonEmpty.reduce(_ union _)
                    println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes do compute action for computeStatisticId = " + computeStatisticId)
                    result.count() //触发job执行
                  } else {
                    println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes found empty result, need no compute action for computeStatisticId = " + computeStatisticId)
                  }
                }

/*
                //Note: 多少个 computeStatistic ，多少个 rdd.count() job
                val rddCount = rddJson.count()  // 可优化，使用 rddJson.isEmpty

                if (rddCount == 0) { //Note: 如果不做判断，空数据时报错， minBy
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes found empty rdd in computeStatisticId = " + computeStatisticId +", rddCount = " + rddCount)
                } else {
                  computesStepConfSeq.foreach {
                  //computesStepConfSeq.par.foreach {
                    case (id, confMap) =>
                      val phaseStep = "computeStatistic.id[" + computeStatisticId +"]-computes.step.id[" + id + "]"
                      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))

                      // compute 步骤输出的结果是 每个 computeStatistic.computes的结果
                      val res = processStep(rddJson, null, sqlc,
                        confMap, cachesPropMap, outputInterfacesPropMap,
                        phaseStep)

                      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process finish processing phaseStep " + phaseStep)
                  }
                }
*/

                rddJson.unpersist()
              })

            } else { //不使用外部缓存，使用内存批次间排重，先执行批次排重，再执行计算统计指标
              //TODO: 处理空数据时 如果不做判断 报错， minBy
              val queueRDDs = batchDeduplicateQueueRDDsMap(computeStatisticId)

              val dStream_lastBatch = ssc.queueStream(queueRDDs, oneAtATime = true)
              val dStream_lastBatch2 = dStream_lastBatch.map(x => (x, x))

              // 约定 batchDeduplicate 每个 computeStatistic 中最多出现一次，且第一个有效
              val (batchDeduplicateStepId, batchDeduplicateStepConfMap) = batchDeduplicateStepConfSeq.head
              val uk = batchDeduplicateStepConfMap("unique.key")
              val uk_for_batchDeduplicate = uk + "_bduk"

              // 用于批次排重的构造流
              val stream2 = stream.transform(rddJson => {
                val df = sqlc.jsonRDD(rddJson)
                val rdd2 =
                  if (df.schema.fieldNames.nonEmpty) {
                    val df_tmp = df.selectExpr(uk + " as " + uk_for_batchDeduplicate, "*")

                    df_tmp.toJSON.map(line => {
                      val jValue = parse(line)
                      val ukValue = compact(jValue \ uk_for_batchDeduplicate)

                      val origin = jValue.removeField {
                        //  case JField(uk_for_batchDeduplicate, _) => true //Note: 不加特殊反引号，会删除所有字段
                        case JField(`uk_for_batchDeduplicate`, _) => true
                        case _ => false
                      }

                      (ukValue, compact(origin))
                    })

                  } else {
                    rddJson.map(x=>(x, x))
                  }
                rdd2
              })

              val stream3 = stream2.leftOuterJoin(dStream_lastBatch2)
              //TODO: 测试内存缓存
              //stream3.persist()

/*
              // TODO: 删除调试信息
              dStream_lastBatch.foreachRDD(rdd_uk=>{
                rdd_uk.foreach(uk=>println(" =  =" * 4 +"[myapp rdd_uk] " + uk))
              })
*/

              stream3.foreachRDD(rddWithUkJsonUk=>{

                rddWithUkJsonUk.persist()

                //获取当前批次中用于批次排重的key
                val rdd_uk = rddWithUkJsonUk.map(_._1).distinct()

                //Note: 重要，不加的话，job的stage会存在很长的依赖关系
                rdd_uk.checkpoint()
                queueRDDs.enqueue(rdd_uk)

                //批次排重
                val rdd2 = rddWithUkJsonUk.filter(_._2._2 == None).map(_._2._1)

                val df = sqlc.jsonRDD(rdd2)
                val schema = df.schema

                if (schema.fieldNames.isEmpty) {
                  //数据为空
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes found empty dataframe after computePrepare in computeStatisticId = " + computeStatisticId)
                } else {
                  //数据不为空
                  val compute_step_rdds =
                    computesStepConfSeq.map {
                      //computesStepConfSeq.par.foreach {
                      case (id, confMap) =>
                        val phaseStep = "computeStatistic.id[" + computeStatisticId + "]-computes.step.id[" + id + "]"
                        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))

                        // compute 步骤输出的结果是 每个 computeStatistic.computes的结果
                        val res = processStep(df, schema, sqlc,
                          confMap, cachesPropMap, outputInterfacesPropMap,
                          phaseStep)

                        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process finish processing phaseStep " + phaseStep)
                        res._1.asInstanceOf[RDD[String]]
                    }

                  val compute_step_rdds_nonEmpty = compute_step_rdds.filter(_.partitions.nonEmpty) // 不使用 rdd.isEmpty 的原因，避免触发job
                  if (compute_step_rdds_nonEmpty.nonEmpty) {
                    val result = compute_step_rdds_nonEmpty.reduce(_ union _)
                    println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes do compute action for computeStatisticId = " + computeStatisticId)
                    result.count() //触发job执行
                  } else {
                    println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes found empty result, need no compute action for computeStatisticId = " + computeStatisticId)
                  }
                }

                rddWithUkJsonUk.unpersist()
              })
            }
          }
          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process finish processing computeStatistics in computeStatisticId " + computeStatisticId)
      }
    }
  }

  def processStep(res_any: Any,
                  res_schema: StructType,
                  sqlc: SQLContext,
                  confMap: Map[String, String],
                  cachesPropMap: Map[String, Map[String, String]] = null,
                  outputInterfacesPropMap: Map[String, Map[String, String]] = null,
                  phaseStep: String = null): (Any, StructType) = {

    val stepType = confMap("step.type")
    val stepMethod = confMap("step.method")
    println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.processStep."+ stepType +" start preocessing phaseStep = " + phaseStep)
    val cachePropMap = confMap.get("cacheId") match {
      case Some(x) if x.nonEmpty => cachesPropMap(x)
      case _ => null
    }

    stepType match {
      case "filter" | "enhance" =>
        stepMethod match {
          case "spark-sql" => //spark-sql方式处理后，输出DataFrame
            val df = res_any match {
              case x: RDD[String] =>
                sqlc.jsonRDD(x, res_schema)
              case x: DataFrame => x
            }

            println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares] df.schema = ")
            //df.printSchema()

            if (df.schema.fieldNames.nonEmpty) {
              val selectExprClause = confMap.getOrElse("selectExprClause", null)
              val whereClause = confMap.getOrElse("whereClause", null)

              val selectUsed = if (selectExprClause != null && selectExprClause.nonEmpty) true else false
              val whereUsed = if (whereClause != null && whereClause.nonEmpty) true else false

              println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares data after stepPhase = " + phaseStep +"], df.rdd.length = " + df.rdd.partitions.length + ", df = ")
              //df.printSchema()
              //df.show()

              val df2 =
                if (selectUsed && whereUsed) {
                  df.selectExpr(selectExprClause.split(","): _*).filter(whereClause)
                } else if (selectUsed && (!whereUsed)) {
                  df.selectExpr(selectExprClause.split(","): _*)
                } else if ((!selectUsed) && whereUsed) {
                  df.filter(whereClause)
                } else df

              println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares data after stepPhase = " + phaseStep +"], df2.rdd.length = " + df2.rdd.partitions.length + ", df2 = ")
              //df2.printSchema()
              //df2.show()
              // TODO: 测试 schmea
              (df2, df2.schema)
            } else { // 空的 DataFrame
              print("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares found empty DataFrame] in stepPhase = " + phaseStep)
              (df, df.schema)
            }

          case "plugin" => //插件类处理后，结构可能变化，
            val plugin = Class.forName(confMap("class")).newInstance().asInstanceOf[StreamingProcessor]

            val rdd2 = res_any match {
              case x: RDD[String] => plugin.process(x, confMap, cachePropMap)
              case x: DataFrame =>
                plugin.process(x.toJSON, confMap, cachePropMap)
            }
            println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares data after stepPhase = " + phaseStep +"], rdd2.partitions.length = " + rdd2.partitions.length)
            (rdd2, null)
        }

      case "compute" =>

        // 计算统计指标的逻辑
        stepMethod match {
          case "spark-sql" => //spark-sql计算统计指标
            val df = res_any match {
              case x: RDD[String] =>
                println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] stepPhase = " + phaseStep + ", res_schema null or not = " + (res_schema == null))
                sqlc.jsonRDD(x, res_schema)
              case x: DataFrame => x
            }

            //df.persist()  // map 操作中不需要，应为在操作最后有 df.unpersist()
            println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] data before stepPhase = " + phaseStep +", df.rdd.length = " + df.rdd.partitions.length + ", df = ")
            //df.printSchema()
            //df.show()

            val result = if (df.schema.fieldNames.nonEmpty) {
              println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] data before stepPhase = " + phaseStep +", df.schema.fieldNames.length = " + df.schema.fieldNames.length)

              val selectExprClause = confMap.getOrElse("selectExprClause", null)
              val whereClause = confMap.getOrElse("whereClause", null)

              val selectUsed = if (selectExprClause != null && selectExprClause.nonEmpty) true else false
              val whereUsed = if (whereClause != null && whereClause.nonEmpty) true else false

              val df2 =
                if (selectUsed && whereUsed) {
                  df.selectExpr(selectExprClause.split(","): _*).filter(whereClause)
                } else if (selectUsed && (!whereUsed)) {
                  df.selectExpr(selectExprClause.split(","): _*)
                } else if ((!selectUsed) && whereUsed) {
                  df.filter(whereClause)
                } else df

              //if (selectUsed || whereUsed) df2.persist()

              println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute data after select and where, stepPhase = " + phaseStep +"], df2.schema.fieldNames.length = " + df2.schema.fieldNames.length + ", df2 = ")
              //df2.printSchema()
              //df2.show()

              if(df2.schema.fieldNames.nonEmpty) {

                //为统计指标设置lebels，便于配置中引用
                val statisticKeyMap = confMap("statisticKeyMap").split(",").map(kvs => {
                  val kv = kvs.trim.split(":")
                  assert(kv.length == 2)
                  (kv(0), kv(1))
                }).toMap

                //设置各个统计指标的统计维度
                val STATSTIC_KEYS_INFO_KEY = "targetKeysList"
                val targetKeysList = confMap(STATSTIC_KEYS_INFO_KEY)

                val aggregateKeyMethodList = confMap("aggegate.key.method.list").split(",").map(ukMethodLabel => {
                  val keyMethodLabelArr = ukMethodLabel.trim.split(":")
                  assert(keyMethodLabelArr.length >= 2)
                  (keyMethodLabelArr(0), keyMethodLabelArr(1))
                })

                val DEFAULT_GROUPBY_NONE_VALUE = "NONE"
                val GROUPBY_NONE_VALUE = confMap.get("groupby.none.key") match {
                  case Some(x) if x.nonEmpty => x
                  case _ => DEFAULT_GROUPBY_NONE_VALUE
                }

                //初始化输出类插件
                val outputClzStr = confMap.getOrElse("output.class", "com.xuetangx.streaming.output.ConsolePrinter")

                val outputConfMap =
                  if (outputClzStr == "com.xuetangx.streaming.output.ConsolePrinter") {
                    Map[String, String]()
                  } else {
                    outputInterfacesPropMap(confMap("output.dataInterfaceId"))
                  }
                val plugin = Class.forName(outputClzStr).newInstance().asInstanceOf[StreamingProcessor]

                println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp  StreamingApp.processStep.compute ] begin computing with targetKeysList = " + targetKeysList)

                val resultSeq_output =
                  targetKeysList.split(",").map(targetKeys => {
                    println("= = " * 10 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp  StreamingApp.processStep.compute ] begin computing with targetKeys = " + targetKeys)

                    val DUMMY_SPLIT_PLACEHOLDER = "DUMMY_SPLIT_PLACEHOLDER"

                    val keyDtypeVtypeArr = (targetKeys + "#" + DUMMY_SPLIT_PLACEHOLDER).split("#").dropRight(1).map(_.trim)
                    val keyLevelListStr = keyDtypeVtypeArr(0)
                    val groupByKeyArr = keyLevelListStr.split(":").map(k => {
                      if (k == DEFAULT_GROUPBY_NONE_VALUE) GROUPBY_NONE_VALUE else statisticKeyMap(k)
                    })

                    val staticKeyLabelExcludes = confMap.get("statistic.keyLabel.excludes") match {
                      case Some(x) if x.nonEmpty => x.split(",").map(_.trim)
                      case _ => Array[String]()
                    }

                    val keyLevelList2 =
                      if (staticKeyLabelExcludes.nonEmpty) {
                        keyDtypeVtypeArr(0).split(":").map(_.trim).filter(!staticKeyLabelExcludes.contains(_))
                      } else {
                        keyDtypeVtypeArr(0).split(":").map(_.trim)
                      }
                    val groupByKeyArr2 =
                      keyLevelList2.map(k => {
                        if (k == DEFAULT_GROUPBY_NONE_VALUE) GROUPBY_NONE_VALUE else statisticKeyMap(k)
                      })

                    val keyDataType = if (keyDtypeVtypeArr.length == 1) confMap("data.type") else keyDtypeVtypeArr(1)
                    val keyValueType = if (keyDtypeVtypeArr.length == 3) keyDtypeVtypeArr(2) else confMap("value.type")
                    val valueCycle = if (keyDtypeVtypeArr.length == 4) keyDtypeVtypeArr(3) else confMap("value.cycle")

                    assert(keyDataType.nonEmpty, "invalid configuration in " + STATSTIC_KEYS_INFO_KEY + ", data.type of " + keyLevelListStr + " is empty")
                    assert(keyValueType.nonEmpty, "invalid configuration in " + STATSTIC_KEYS_INFO_KEY + ", value.type of " + keyLevelListStr + " is empty")
                    assert(valueCycle.nonEmpty, "invalid configuration in " + STATSTIC_KEYS_INFO_KEY + ", value.cycle of " + keyLevelListStr + " is empty")

                    // pre_output 需要的配置信息: 统计指标维度，data_type(指标关联实体标识), value_type(指标取值含义)
                    val preOutputConfMap = outputConfMap ++
                            Map[String, String](
                              "origin.statistic.key" -> groupByKeyArr.mkString(","),
                              "origin.statistic.keyLevel" -> keyLevelListStr,
                              "statistic.key" -> (if (keyLevelList2.nonEmpty) keyLevelList2.mkString(",") else "L0"),
                              "statistic.keyName" -> (if (groupByKeyArr2.nonEmpty) groupByKeyArr2.mkString(",") else "NONE"),
                              "statistic.data_type" -> keyDataType,
                              "statistic.value_type" -> keyValueType,
                              "statistic.cycle" -> valueCycle
                            )

                    val rdd_stat_arr = aggregateKeyMethodList.map { case (key, method) =>

                      println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] using spark sql dataframe, df2.printSchema() = ")
                      //df2.printSchema()
                      //df2.show()

                      val rdd_stat = method match {
                        case "count" =>
                          //println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute using count before union] df2.rdd.partitions.length = " + df2.rdd.partitions.length + ", groupByKeyArr = " + groupByKeyArr.mkString("[", ",", "]"))

                          if (groupByKeyArr(0) == GROUPBY_NONE_VALUE) {
                            // groupByKey为 None，取当前批次数据的记录数
                            df2.agg(count(key).alias("value")).toJSON
                          } else {
                            df2.groupBy(groupByKeyArr(0), groupByKeyArr.drop(1): _*).agg(count(key).alias("value")).toJSON
                          }
                        case "countDistinct" =>
                          println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute using countDistinct before union] df2.rdd.partitions.length = " + df2.rdd.partitions.length + ", groupByKeyArr = " + groupByKeyArr.mkString("[", ",", "]"))

                          if (groupByKeyArr(0) == GROUPBY_NONE_VALUE) {
                            df2.agg(countDistinct(key).alias("value")).toJSON
                          } else {
                            df2.groupBy(groupByKeyArr(0), groupByKeyArr.drop(1): _*).agg(countDistinct(key).alias("value")).toJSON
                          }
                      }

                      //println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] after compute , groupByKeyArr = " + groupByKeyArr.mkString("[", ",", "]") +", rdd_stat = " )

                      //增加 data_type, value_type 等信息到统计指标的json中，是否放在 output插件类中（放到插件类需要一些配置）
                      plugin.pre_output(rdd_stat, preOutputConfMap)
                    }

                    val rdd_stat_union = rdd_stat_arr.reduce(_ union _)

                    //触发job的action操作
                    // Note: union 统计结果的 rdd，可以减少 job 的数量，增加 partition 数量，增加任务并行
                    val rdd_output = plugin.output(rdd_stat_union, preOutputConfMap)
                    rdd_output
                  })

                //if (selectUsed || whereUsed) df2.unpersist()

                //TODO: 更新类型
                (resultSeq_output.reduce(_ union _), null)
              } else {
                println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute ] found empty dataFrame after phaseStep = " + phaseStep)
                //(null, null)
                (sqlc.sparkContext.emptyRDD, null)
              }

            } else {
              println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] found empty dataFrame before compute phaseStep = " + phaseStep)
              (sqlc.sparkContext.emptyRDD, null)
            }

            //df.unpersist()

            result

          case "plugin" => //插件类计算统计指标
            //TODO: 支持插件类方式计算统计指标

            val plugin = Class.forName(confMap("class")).newInstance().asInstanceOf[StreamingProcessor]
            val rdd_stat = res_any match {
              case x: RDD[String] => plugin.compute(x, confMap)
              case x: DataFrame =>
                //TODO: 支持更好的控制
                plugin.compute(x.toJSON, confMap)
            }

            //触发job的action操作
            val outputClzStr = confMap.getOrElse("output.class", "com.xuetangx.streaming.output.ConsolePrinter")
            val outputConfMap =
              if (outputClzStr == "com.xuetangx.streaming.output.ConsolePrinter") {
                Map[String, String]()
              } else {
                outputInterfacesPropMap(confMap("output.dataInterfaceId"))
              }

            val outputPlugin = Class.forName(outputClzStr).newInstance().asInstanceOf[StreamingProcessor]

            val rdd_output = outputPlugin.output(rdd_stat, outputConfMap)

            //TODO: 支持插件类方式计算统计指标
            //throw new Exception("does not support to compute statics using plugin class")
            (rdd_output, null)
        }
        //println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.processStep finished preocessing phaseStep = " + phaseStep)
    }
  }
}
