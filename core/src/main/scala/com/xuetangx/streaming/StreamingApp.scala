package com.xuetangx.streaming

import com.xuetangx.streaming.monitor.MConsolePrinter
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
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
    /*
        val (appsCommonPropMap, appsPropMap) = parseProperties2Map(conf\ "apps", "app", "id")
        val (dataSourcesCommonPropMap, dataSourcesPropMap) = parseProperties2Map(conf \ "dataSources", "source", "id")
        val (dataInterfaceCommonPropMap, dataInterfacesPropMap) = parseProperties2Map(conf \ "dataInterfaces", "interface", "id")
        val (cacheCommonPropMap, cachesPropMap) = parseProperties2Map(conf\ "externalCaches", "cache", "id")
    */

//    val monitor = Class.forName("com.xuetangx.streaming.monitor.MConsolePrinter").newInstance()

    // 应用的配置
    val appsPropMap = Utils.parseProperties2Map(conf\ "apps", "app", "app.id")
    val interfaceId = appsPropMap(appId)("app.interfaceId") //获取输入接口id

    // 数据源的配置
    val dataSourcesPropMap = Utils.parseProperties2Map(conf \ "dataSources", "source", "source.id")

    // 数据接口的配置，分2类： input, output
    val dataInterfacesPropMap = Utils.parseProperties2Map(conf \ "dataInterfaces", "interface", "interface.id")
    // 输出接口的配置
    val outputDataInterfacesPropMap = dataInterfacesPropMap.filter{case (id, mapConf) => mapConf("interface.type") == "output"}
    val outputInterfacesPropMap = outputDataInterfacesPropMap.map{case (id, mapConf) => 
      (id, mapConf ++ dataSourcesPropMap(dataInterfacesPropMap(id)("interface.sourceId")))
    }

    // 外部缓存的配置
    val cachesPropMap = Utils.parseProperties2Map(conf\ "externalCaches", "cache", "cache.id")

    // 指定的输入接口配置（含数据源信息）
    val inputInterfacePropMap = dataInterfacesPropMap(interfaceId) ++
            dataSourcesPropMap(dataInterfacesPropMap(interfaceId)("interface.sourceId"))

    // 准备阶段配置
    val preparesConf = (conf \ "prepares").filter(_.attribute("interfaceId").get.text==interfaceId)
    //    val (preparesCommonPropMap, preparesPropMap) = parseProperties2Map(preparesConf, "prepare", "id") //报错
    val preparesPropMap = Utils.parseProperties2Map(preparesConf, "step", "step.id")

    // 指定的计算阶段配置
    val computeStatisticsConf = (conf \ "computeStatistics").filter(node=>{
      //      node.attribute("interfaceId")==interfaceId
      node.attribute("interfaceId").get.text==interfaceId
    })

    // 指定数据接口id计算统计指标的计算配置(计算统计指标的数据集id->是否启用->指定id的配置)
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
    val preparesActivePropSeq = preparesPropMap.filter{case (k, v)=>
      v.getOrElse("prepare.enabled", "false") == "true"
    }.toSeq.sortWith(_._1 < _._1)

    // 指定数据接口id有效的计算配置
    val computesActiveConfTupleSeq = computesConfTuple.filter(_._2=="true").map(kxv=>{
      val (key, flag, outerMap) = kxv

      val filteredStepsMap = outerMap.filter{case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"}
      (key, filteredStepsMap.toSeq.sortWith(_._1 < _._1))
    }).sortWith(_._1 < _._1)


    // 初始化
    val sparkConf = new SparkConf().setMaster(master).setAppName(appName)
    val sc = new SparkContext(sparkConf)
    appsPropMap(appId).get("checkpointDir") match {
      case Some(x) if x.nonEmpty => sc.setCheckpointDir(x)
      case _ =>
    }

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
      preparesActivePropSeq, computesActiveConfTupleSeq)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
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
              preparesActivePropSeq: Seq[(String, Map[String, String])],
              computesActiveConfTupleSeq: Seq[(String, Seq[(String, Map[String, String])])]): Unit = {

    // 初始化批次排重数据结构
    val batchDeduplicateQueueRDDsMap = mutable.Map[String, mutable.Queue[RDD[String]]]()

    val batchDeduplicateConfSeq = computesActiveConfTupleSeq.map{case (k1, stepConfSeq) => {
      val hasBatchDeduplicateStep = stepConfSeq.map{case (k2, stepConf) => {
        if (stepConf("step.type") == "batchDeduplicate") "true" else "false"
      }}.contains("true")
      (k1, hasBatchDeduplicateStep)
    }}.filter(_._2)

    batchDeduplicateConfSeq.map(_._1).foreach(id=>{
      batchDeduplicateQueueRDDsMap.put(id, new mutable.Queue[RDD[String]]())
    })

    // 准备阶段处理
    // Note: 准备阶段不支持 batchDeduplicate 步骤
    //  准备阶段处理1
    val dStream2 =
      if (inputInterfaceConfMap("type") == "json") dStream
      else {
        //TODO: 支持非json形式的日志处理，转换为json形式的日志
        throw new Exception("Error: unsupport to analyze non-json type log")
      }

/*
    val dStream2 = dStream.transform(rdd => {
      //转换日志为DataFrame
      //  是否需要转换为dataFrame，可以根据 prepares 中是否使用 spark-sql过滤/增强确定
      val rddInJson =
        if (inputInterfaceConfMap("type") == "json") {
          //TODO: 优化，允许指定schema
          rdd
        } else {
          //TODO: 支持非json形式的日志处理，转换为json形式的日志
          throw new Exception("Error: unsupport to analyze non-json type log")
        }

      val df = sqlc.jsonRDD(rddInJson)
      res_schema = df.schema
      println("= = " * 20 +"[myapp] res_schema = " + res_schema.treeString)
      res_schema.printTreeString()

      println("= = " * 20 + "[myapp] df.schema.fieldNames.length = " + df.schema.fieldNames.length)
      val rdd2 =
        if (df.schema.fieldNames.nonEmpty) {
          //没有数据时快速跳过job执行
          println("= = " * 20 + "[myapp] df.schema.fieldNames.nonEmpty = true ")
          //Note: 准备，分多个步骤
          // 分2类，过滤，增强；
          //  过滤1(过滤方式：spark-sql, plugin)
          //  增强1(在之前的基础上)

          res_any = df
          res_schema = df.schema

          rddInJson
        } else {
          rddInJson
        }
      rdd2
    })
*/

    // Note: dStream_after_prepares 中的 rdd 是json形式
    val dStream_after_prepares =
      if (preparesActivePropSeq.isEmpty) { //准备阶段没有配置步骤
        dStream2
      } else { //准备阶段配置了执行步骤

        dStream2.transform(rddJson => {
          val df = sqlc.jsonRDD(rddJson)

//          println("= = " * 20 + "[myapp] df.schema.length = " + df.schema.length)
          MConsolePrinter.output("df.schema.length = " + df.schema.length, "StreamingApp.process", "= = " * 10)

          val rdd2 =
            if (df.schema.fieldNames.nonEmpty) {
              //没有数据时快速跳过job执行
              println("= = " * 20 + "[myapp] df.schema.fieldNames.nonEmpty = true ")
              //Note: 准备，分多个步骤
              // 分2类，过滤，增强；
              //  过滤和增强都有2种方式：spark-sql, plugin

              // 初始化准备阶段每个步骤输出结果
              var res_any: Any = df
              var res_schema: StructType = df.schema

              // 遍历准备阶段准备步骤
              preparesActivePropSeq.foreach {
                case (id, confMap) =>
                  val phaseStep = "prepares.id[" + confMap("prepares.id") +"]-prepare.id[" + id +"]"
                  logInfo("[myapp] start processing prepares" + preparesActivePropSeq.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))
                  println("= = " * 20 + "[myapp] res_any is RDD or not = " + res_any.isInstanceOf[RDD[String]] + ", prepares.step.id = " + id)
                  println("= = " * 20 + "[myapp] res_any is DataFrame or not = " + res_any.isInstanceOf[DataFrame] + ", prepares.step.id = " + id)
                  val res = processStep(res_any, res_schema, sqlc,
                    confMap, outputInterfacesPropMap,
                    phaseStep)
                  res_any = res._1
                  res_schema = res._2
                  logInfo("[myapp] finish processing prepares" + preparesActivePropSeq.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep)
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
    //  TODO: 1输出
    //Note:
    // TODO: 2如果 computesActiveConfTupleSeq 为空时的处理； 3是否支持并发
    if(computesActiveConfTupleSeq.isEmpty) { //配置中没有配置有效的计算，不需要触发job
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
      val id2streams = computesActiveConfTupleSeq.map {
        case (computeStatisticId, stepsConfSeq) =>
          //Note: 每个 computeStatisticId 对应一些过滤条件，
          //   应该有各自的 res_any_in_computeStatistic, res_schema_in_computeStatistic, 初始化可以是准备阶段的值,
          //   但是问题是定义时不在 DStream.transform中执行，有问题，所以初始化为null
          // 输入：dStream_after_prepares
          // 输出：

          logInfo("[myapp] start processing computeStatistics " + stepsConfSeq.map(_._1).mkString("[", ",", "]") + " in computeStatisticId " + computeStatisticId)

          // 处理计算阶段流程，准备步骤 - 1先处理filter 和 enhance
          val preparesStepsConfSeq = stepsConfSeq.filterNot {
            case (id, confMap) => confMap("step.type") == "compute" || confMap("step.type") == "batchDeduplicate"
          }

          // 计算每个有效统计指标集合——先执行准备步骤，再计算，最后输出
          val dStream3 = dStream_after_prepares.transform(rddJson => {
            var res_any_in_computeStatistic: Any = rddJson
            var res_schema_in_computeStatistic: StructType = null

            // 遍历计算阶段准备步骤
            preparesStepsConfSeq.foreach {
              case (id, confMap) =>
                val phaseStep = "computeStatistic.id[" + computeStatisticId +"]-step.id[" + id + "]"
                logInfo("[myapp] start processing computeStatistic" + stepsConfSeq.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))

                println("= = " * 20 + "[myapp] res_any is RDD or not = " + res_any_in_computeStatistic.isInstanceOf[RDD[String]] + ", computeStatistic.step.id = " + id + ", computeStatisticId = " + computeStatisticId)
                println("= = " * 20 + "[myapp] res_any is DataFrame or not = " + res_any_in_computeStatistic.isInstanceOf[DataFrame] + ", computeStatistic.step.id = " + id + ", computeStatisticId = " + computeStatisticId)
                val res = processStep(res_any_in_computeStatistic, res_schema_in_computeStatistic, sqlc,
                  confMap, outputInterfacesPropMap,
                  phaseStep)
                res_any_in_computeStatistic = res._1
                res_schema_in_computeStatistic = res._2

                logInfo("[myapp] finish processing computeStatistic" + stepsConfSeq.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep)
            }

            res_any_in_computeStatistic match {
              case x: RDD[String] => x
              case x: DataFrame => x.toJSON
            }
          })
          (computeStatisticId, dStream3)
      }

      // 处理计算阶段流程，计算步骤 - 3 批次去重 + 计算统计指标
      id2streams.par.foreach{
        case (computeStatisticId, stream) =>

          val computesStepConfSeq = computesActiveConfTupleSeq.toMap.get(computeStatisticId).get.filter { case (id, confMap) => confMap("step.type") == "compute" }
          val batchDeduplicateStepConfSeq = computesActiveConfTupleSeq.toMap.get(computeStatisticId).get.filter { case (id, confMap) => confMap("step.type") == "batchDeduplicate" }

          if (computesStepConfSeq.isEmpty) { //配置中没有配置有效的计算统计指标，不需要触发job
            stream.foreachRDD(rddJson => {
              logWarning("[myapp configuration] found no active computeStatistic.computes in computeStatistics part")
              // rddJson.filter(line => false).count()
              rddJson
            })
          } else { //存在有效的计算配置
            if(batchDeduplicateStepConfSeq.isEmpty) { // 没有配置批次排重，直接执行计算统计指标步骤
              //TODO: 测试内存缓存
              stream.persist()

              // stream： DStream[String]
              stream.foreachRDD(rddJson => {
                
                computesStepConfSeq.foreach {
                  case (id, confMap) =>
                    val phaseStep = "computeStatistic.id[" + computeStatisticId +"]-step.id[" + id + "]"
                    logInfo("[myapp] start processing computeStatistic.computes" + computesStepConfSeq.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))

                    // compute 步骤输出的结果是 每个 computeStatistic.computes的结果
                    // TODO: 持久化是放在 processStep 内，还是 processStep 外
                    //TODO: 需要删除
//                    println("= = " * 20 + "[myapp] res_any is RDD or not = " + res_any.isInstanceOf[RDD[String]] + ", computeStatistic.step.id = " + id + ", computeStatisticId = " + computeStatisticId)
//                    println("= = " * 20 + "[myapp] res_any is DataFrame or not = " + res_any.isInstanceOf[DataFrame] + ", computeStatistic.step.id = " + id + ", computeStatisticId = " + computeStatisticId)
                    val res = processStep(rddJson, null, sqlc,
                      confMap, outputInterfacesPropMap,
                      phaseStep)

                    logInfo("[myapp] finish processing computeStatistic.computes" + computesStepConfSeq.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep)
                }
              })

            } else { //配置了批次排重，先执行批次排重，再执行计算统计指标
              val queueRDDs = batchDeduplicateQueueRDDsMap(computeStatisticId)

              val dStream_lastBatch = ssc.queueStream(queueRDDs, oneAtATime = true)
//              dStream_lastBatch.checkpoint("")
                      //TODO: 测试内存缓存
//                      .persist()
              val dStream_lastBatch2 = dStream_lastBatch.map(x => (x, x))

              // 约定 batchDeduplicate 每个 computeStatistic 中最多出现一次，且第一个有效
              val (batchDeduplicateStepId, batchDeduplicateStepConfMap) = batchDeduplicateStepConfSeq.head
              val uk = batchDeduplicateStepConfMap("unique.key")
              val uk_for_batchDeduplicate = uk + "_bduk"

              var res_schema_in_computeStatistic2: StructType = null

              // 用于批次排重的构造流
              val stream2 = stream.transform(rddJson => {
                val df = sqlc.jsonRDD(rddJson)
                val rdd2 =
                  if (df.schema.fieldNames.nonEmpty) {
                    val df_tmp = df.selectExpr(uk + " as " + uk_for_batchDeduplicate, "*")

                    // TODO: 删除DEBUG信息
//                    df_tmp.printSchema()
                    df_tmp.show()

                    res_schema_in_computeStatistic2 = df.schema

                    df_tmp.toJSON.map(line => {
                      val jValue = parse(line)
                      val ukValue = compact(jValue \ uk_for_batchDeduplicate)

                      val origin = jValue.removeField {
                        //  case JField(uk_for_batchDeduplicate, _) => true //Note: 不加特殊反引号，会删除所有字段
                        case JField(`uk_for_batchDeduplicate`, _) => true
                        case _ => false
                      }

                      //TODO: 删除调试信息
//                      println("= = " * 10 + "[myapp batchDeduplicate current batch data] #(" + ukValue + "," + compact(origin) + ")#")
                      (ukValue, compact(origin))
                    })

                  } else {
                    rddJson.map(x=>(x, x))
//                    sqlc.sparkContext.parallelize(Array[(String,String)](), 1)
                  }
                rdd2
              })

              val stream3 = stream2.leftOuterJoin(dStream_lastBatch2)
              //TODO: 测试内存缓存
              stream3.persist()

              // TODO: 删除调试信息
              dStream_lastBatch.foreachRDD(rdd_uk=>{
                rdd_uk.foreach(uk=>println(" =  =" * 20 +"[myapp rdd_uk] " + uk))
              })

              stream3.foreachRDD(rddWithUkJsonUk=>{
//              stream3.foreachRDD(rddWithUkJsonUk=>{
                //获取当前批次中用于批次排重的key
                val rdd_uk = rddWithUkJsonUk.map(_._1).distinct()

                //Note: 重要，不加的话，job的stage会存在很长的依赖关系
                rdd_uk.persist()
                rdd_uk.checkpoint()
                queueRDDs.enqueue(rdd_uk)


                //TODO: 删除调试信息
                rddWithUkJsonUk.foreach(x=>println("= = " * 20 +"[myapp join rddWithUkJsonUk] " + (x._1, x._2._2, x._2._1)))

                //批次排重
                val rdd2 = rddWithUkJsonUk.filter(_._2._2 == None).map(_._2._1)
                if (rdd2.partitions.nonEmpty) {
                  // 计算统计指标
                  computesStepConfSeq.foreach {
                    case (id, confMap) =>
                      val phaseStep = "computeStatistic.id[" + computeStatisticId +"]-step.id[" + id + "]"

                      logInfo("[myapp] start processing computeStatistic.computes" + computesStepConfSeq.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep + " with config = " + confMap.mkString("[", ",", "]"))

                      // compute 步骤输出的结果是 每个 computeStatistic.computes的结果
                      // TODO: 持久化是放在 processStep 内，还是 processStep 外
                      // TODO: 需要删除
//                      println("= = " * 20 + "[myapp] res_any is RDD or not = " + res_any.isInstanceOf[RDD[String]] + ", computeStatistic.step.id = " + id + ", computeStatisticId = " + computeStatisticId)
//                      println("= = " * 20 + "[myapp] res_any is DataFrame or not = " + res_any.isInstanceOf[DataFrame] + ", computeStatistic.step.id = " + id + ", computeStatisticId = " + computeStatisticId)
                      val res = processStep(rdd2, res_schema_in_computeStatistic2, sqlc,
                        confMap, outputInterfacesPropMap,
                        phaseStep)

                      logInfo("[myapp] finish processing computeStatistic.computes" + computesStepConfSeq.map(_._1).mkString("[", ",", "]") + ", phaseStep " + phaseStep)
                  }
                }
              })
            }
          }
          logInfo("[myapp] finish processing computeStatistics in computeStatisticId " + computeStatisticId)
      }
    }
  }

  def processStep(res_any: Any,
                  res_schema: StructType,
                  sqlc: SQLContext,
                  confMap: Map[String, String],
                  outputInterfacesPropMap: Map[String, Map[String, String]] = null,
                  phaseStep: String = null): (Any, StructType) = {
    
    logInfo("[myapp] processStep start preocessing phaseStep = " + phaseStep)
    val stepType = confMap("step.type")
    val stepMethod = confMap("step.method")

    stepType match {
      case "filter" | "enhance" =>
        stepMethod match {
          case "spark-sql" => //spark-sql方式处理后，输出DataFrame
            val df = res_any match {
              case x: RDD[String] =>
                sqlc.jsonRDD(x, res_schema)
              case x: DataFrame => x
            }

            if (df.schema.fieldNames.nonEmpty) {
              val selectExprClause = confMap.getOrElse("selectExprClause", null)
              val whereClause = confMap.getOrElse("whereClause", null)

              val selectUsed = if (selectExprClause != null && selectExprClause.nonEmpty) true else false
              val whereUsed = if (whereClause != null && whereClause.nonEmpty) true else false

              val df2 = if (selectUsed && whereUsed){
                df.selectExpr(selectExprClause).filter(whereClause)
              } else if (selectUsed && (! whereUsed)) {
                df.selectExpr(selectExprClause)
              } else if ((! selectUsed) && whereUsed) {
                df.filter(whereClause)
              } else df

              println("= = " * 10 + "[myapp data after stepPhase = " + phaseStep +"], df.rdd.length = " + df.rdd.partitions.length + ", dataFrame = ")
              df2.show()
              (df2, df2.schema)
            } else { // 空的 DataFrame
              logInfo("= = " * 10 + "[myapp found empty DataFrame] in stepPhase = " + phaseStep)
              (df, df.schema)
            }

          case "plugin" => //插件类处理后，结构可能变化，
            val plugin = Class.forName(confMap("class")).newInstance().asInstanceOf[StreamingProcessor]

            //TODO: 如何判断数据为空的情况
            val rdd2 = res_any match {
              case x: RDD[String] => plugin.process(x, confMap)
              case x: DataFrame =>
                //TODO: 支持更好的控制
                plugin.process(x.toJSON, confMap)
            }
            println("= = " * 10 + "[myapp data after stepPhase = " + phaseStep +"], rdd.length = " + rdd2.partitions.length)
            (rdd2, null)
        }

      case "compute" =>

        // 构造保存统计指标的rdd
        var rdd_result = sqlc.sparkContext.parallelize(Array[String]())
//        println("= = " * 20 + "[myapp  processStep.compute union] initial rdd_result.partitions.length = " + rdd_result.partitions.length)

        //TODO: 计算统计指标的逻辑
        stepMethod match {
          case "spark-sql" => //spark-sql计算统计指标
            val df = res_any match {
              case x: RDD[String] => sqlc.jsonRDD(x, res_schema)
              case x: DataFrame => x
            }

            if(df.schema.fieldNames.nonEmpty) {
              df.persist()
              //为统计指标设置lebels，便于配置中引用
              val statisticKeyMap = confMap("statisticKeyMap").split(",").map(kvs => {
                val kv = kvs.trim.split(":")
                assert(kv.length == 2)
                (kv(0), kv(1))
              }).toMap

              //设置各个统计指标的统计维度
              val targetKeysList = confMap("targetKeysList")

              val ukMethodLabelList = confMap("uk.method.label.list").split(",").map(ukMethodLabel => {
                val ukMethodLabelArr = ukMethodLabel.trim.split(":")
                assert(ukMethodLabelArr.length == 3)
                (ukMethodLabelArr(0), ukMethodLabelArr(1), ukMethodLabelArr(2))
              })

              //初始化输出类插件
              val outputClzStr = confMap.getOrElse("output.class", "com.xuetangx.streaming.output.ConsolePrinter")

              val outputConfMap =
                if (outputClzStr == "com.xuetangx.streaming.output.ConsolePrinter") {
                  Map[String, String]()
                } else {
                  outputInterfacesPropMap(confMap("output.dataInterfaceId"))
                }
              val plugin = Class.forName(outputClzStr).newInstance().asInstanceOf[StreamingProcessor]
              logInfo("[myapp processStep.compute ] output result of statistics")


              for (targetKeys <- targetKeysList.split(",")) {
                val groupByKeys = targetKeys.split(":").map(statisticKeyMap(_))
                ukMethodLabelList.foreach { case (key, method, label) =>
                  val rdd_stat =  method match {
                    case "count" =>
                      //TODO: 删除调试信息
                      println("= = " * 20 + "[myapp  processStep.compute.count before union] df.rdd.partitions.length = " + df.rdd.partitions.length)
//                      rdd_result = rdd_result.union(df.groupBy(groupByKeys(0), groupByKeys.drop(1): _*).agg(count(key).alias("value")).toJSON)
                      df.groupBy(groupByKeys(0), groupByKeys.drop(1): _*).agg(count(key).alias("value")).toJSON
                    //            rdd = rdd.union()
//                      println("= = " * 20 + "[myapp  processStep.compute.count after union] rdd_result.partitions.length = " + rdd_result.partitions.length)

                    case "countDistinct" =>
                      println("= = " * 20 + "[myapp  processStep.compute.countDistinct before union] df.rdd.partitions.length = " + df.rdd.partitions.length)
//                      rdd_result = rdd_result.union(df.groupBy(groupByKeys(0), groupByKeys.drop(1): _*).agg(countDistinct(key).alias("value")).toJSON)
                      df.groupBy(groupByKeys(0), groupByKeys.drop(1): _*).agg(countDistinct(key).alias("value")).toJSON
//                      println("= = " * 20 + "[myapp  processStep.compute.countDistinct after union] rdd_result.partitions.length = " + rdd_result.partitions.length)
                  }

                  //增加统计指标的维度到配置(keys=groupByKeys.mkString(","))


                  //触发job的action操作
                  //rdd_result.foreach(line => println("= = " * 10 + "[myapp output] " + line))
                  //            rdd_result.count()
                  //            val cnt = rdd_result.count()
                  //            println("= = " * 20 +" cnt = " + cnt)
                  rdd_result = rdd_result.union(rdd_stat)
                  plugin.output(rdd_stat, outputConfMap + ("id.keyNames"->groupByKeys.mkString(",")))
                }
              }



              df.unpersist()
            } else {
              logInfo("[myapp processStep.compute ] found empty dataFrame before compute phaseStep = " + phaseStep)
            }

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
            //rdd_result.foreach(line => println("= = " * 10 + "[myapp output] " + line))
            //            val cnt = rdd_result.count()
            //            println("= = " * 20 +" cnt = " + cnt)
            val outputClzStr = confMap.getOrElse("output.class", "com.xuetangx.streaming.output.ConsolePrinter")
            val outputConfMap =
              if (outputClzStr == "com.xuetangx.streaming.output.ConsolePrinter") {
                Map[String, String]()
              } else {
                outputInterfacesPropMap(confMap("output.dataInterfaceId"))
              }

            val outputPlugin = Class.forName(outputClzStr).newInstance().asInstanceOf[StreamingProcessor]

            rdd_result = rdd_result.union(rdd_stat)
            outputPlugin.output(rdd_stat, outputConfMap)

//            throw new Exception("does not support to compute statics using plugin class")
        }

        logInfo("[myapp] processStep finished preocessing phaseStep = " + phaseStep)
        //        (res_any, res_schema)
        (rdd_result, null)
    }
  }
}
