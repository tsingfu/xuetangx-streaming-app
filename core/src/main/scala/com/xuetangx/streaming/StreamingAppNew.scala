package com.xuetangx.streaming

/**
 * Created by tsingfu on 15/11/29.
 */
import com.xuetangx.streaming.util.{DateFormatUtils, SparkUtils, Utils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.xml.XML

object StreamingAppNew {

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

    // 指定的输入接口配置（含数据源信息）
    val inputInterfacePropMap = dataInterfacesPropMap(interfaceId) ++
            dataSourcesPropMap(dataInterfacesPropMap(interfaceId)("interface.sourceId"))

    // 输出接口的配置
    val dataInterfacesPropMap_output = dataInterfacesPropMap.filter{case (id, confMap) => confMap("interface.type") == "output"}
    val outputInterfacesPropMap = dataInterfacesPropMap_output.map{case (id, confMap) =>
      (id, confMap ++ dataSourcesPropMap(dataInterfacesPropMap(id)("interface.sourceId")))
    }

    val di_confs_with_ds_conf = dataInterfacesPropMap.map{case (id, confMap) =>
      (id, confMap ++ dataSourcesPropMap(dataInterfacesPropMap(id)("interface.sourceId")))
    }

    // 外部缓存的配置
    //    val cachesPropMap = Utils.parseProperties2Map(conf\ "externalCaches", "cache", "cache.id")
    val cachesPropMap = Utils.parseProperties2Map(conf\ "externalCaches", "cache", "cache.id").map{case (cacheId, confMap)=>
      (cacheId, confMap ++ dataSourcesPropMap(confMap("cache.sourceId")))
    }

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
    // 准备阶段配置中有效步骤的配置
    val preparesPropSeq_active = preparesPropMap.filter{case (k, v)=>
      v.getOrElse("step.enabled", "false") == "true"
    }.toSeq.sortWith(_._1 < _._1)

    // TODO: 新封装
    val preparesRule_active = preparesPropMap.filter{case (k, v)=>
      v.getOrElse("step.enabled", "false") == "true"
    }.map{case (ruleId, ruleConf) =>
      //val ruleType = conf("step.type")
      val ruleMethod = ruleConf("step.method")
      // val ruleEnabled = conf("step.enabled")

      val clz_name = ruleConf.getOrElse("class", "com.xuetangx.streaming.StreamingRDDRule")
      val rule = Class.forName(clz_name).newInstance().asInstanceOf[StreamingRDDRule]
      ruleMethod match {
        case "spark-sql" =>
          rule.init(ruleId, ruleConf)
          rule
        case "plugin" =>
          val cacheConf = ruleConf.get("cacheId") match {
            case Some(cacheId) if cacheId.trim.nonEmpty => cachesPropMap(cacheId.trim)
            case _ => Map[String, String]()
          }

          val outputConf = ruleConf.get("output.dataInterfaceId") match {
            case Some(di_output_id) if di_output_id.trim.nonEmpty =>
              di_confs_with_ds_conf(di_output_id.trim)
            case _ => Map[String, String]()
          }

          rule.init(ruleId, ruleConf, cacheConf, outputConf)
          rule
        case _ =>
          throw new Exception("unsupported step.method " + ruleMethod)
      }
    }.toSeq.sortWith(_.ruleId < _.ruleId)


    println("=  = " * 10 + "[myapp configuration] preparesPropSeq_active for interfaceId = " + interfaceId + ", preparesPropSeq_active = ")
    preparesPropSeq_active.foreach{case (k, vMap) =>
      println("- - " * 10)
      println("stepId = " + k +", stepConf = " + vMap.mkString("[", ",\n", "]"))
    }

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

      //println("=  = " * 10)
      //println(outerAttrMap.mkString("[", ",", "]"))
      (outerAttrMap("id"), outerAttrMap("enabled"), compute_preparesConfMap)
    }

    // compute_computesConfTuple2: Seq[(computeStatistic.id, computeStatistic.enabled, computesConfMap)]
    //  其中 computesConfMap： Map[step.id, Map[String, String]
    val compute_computesConfTuple = for (computeStatisticConf <- computeStatisticsConf \ "computeStatistic") yield {
      val outerAttrMap = computeStatisticConf.attributes.asAttrMap

      val compute_computesConfMap = Utils.parseProperties2Map(computeStatisticConf \ "computes", "step", "step.id").map{case (k, vMap)=>
        (k, vMap ++ outerAttrMap.map{case (outKey, v) => (computeStatisticConf.head.label+"."+outKey, v)})
      }

      //println("=  = " * 10)
      //println(outerAttrMap.mkString("[", ",", "]"))

      // (computeStatistic.id, computeStatistic.enabled, computesConfMap)
      (outerAttrMap("id"), outerAttrMap("enabled"), compute_computesConfMap)
    }

    // 指定数据接口id有效的计算配置
    val compute_preparesConfMap_active = compute_preparesConfTuple.filter(_._2=="true").map(kxv=>{
      val (computeStatisticId, flag, stepConfMap) = kxv

      val filteredStepsMap = stepConfMap.filter{case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"}

      println("=  = " * 10 + "[myapp configuration] compute_preparesConfMap_active for computeStatisticId = " + computeStatisticId + ", compute_preparesConfMap_active = ")
      //println(filteredStepsMap.mkString("[", ",", "]"))
      filteredStepsMap.foreach{case (k, vMap) =>
        println("- - " * 10)
        println("stepId = " + k +", stepConf = " + vMap.mkString("[", ",\n", "]"))
      }

      (computeStatisticId, filteredStepsMap.toSeq.sortWith(_._1 < _._1))
    }).toMap


    //TODO: 新封装
    val compute_preparesRule_active = compute_preparesConfTuple.filter(_._2=="true").map(kxv=>{
      val (computeStatisticId, flag, stepConfMap) = kxv

//      val filteredStepsMap = stepConfMap.filter{case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"}
//
//      println("=  = " * 10 + "[myapp configuration] compute_preparesConfMap_active for computeStatisticId = " + computeStatisticId + ", compute_preparesConfMap_active = ")
//      //println(filteredStepsMap.mkString("[", ",", "]"))
//      filteredStepsMap.foreach{case (k, vMap) =>
//        println("- - " * 10)
//        println("stepId = " + k +", stepConf = " + vMap.mkString("[", ",\n", "]"))
//      }

      val rules = stepConfMap.filter{
        case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"
      }.map{case (ruleId, ruleCconf) =>
        //val ruleType = conf("step.type")
        val ruleMethod = ruleCconf("step.method")
        // val ruleEnabled = conf("step.enabled")
        val clz_name = appsPropMap(appId).getOrElse("class", "com.xuetangx.streaming.StreamingRDDRule")
        val rule = Class.forName(clz_name).newInstance().asInstanceOf[StreamingRDDRule]

        ruleMethod match {
          case "spark-sql" =>
            rule.init(ruleId, ruleCconf)
            rule
          case "plugin" =>
            val cacheConf = ruleCconf.get("cacheId") match {
              case Some(cacheId) if cacheId.trim.nonEmpty => cachesPropMap(cacheId.trim)
              case _ => null
            }

            val outputConf = ruleCconf.get("output.dataInterfaceId") match {
              case Some(di_output_id) if di_output_id.trim.nonEmpty =>
                di_confs_with_ds_conf(di_output_id.trim)
              case _ => null
            }

            rule.init(ruleId, ruleCconf, cacheConf, outputConf)
            rule
          case _ =>
            throw new Exception("unsupported step.method " + ruleMethod)
        }
      }.toSeq.sortWith(_.ruleId < _.ruleId)

      (computeStatisticId, rules)
    }).toMap


    val compute_computesConfMap_active = compute_computesConfTuple.filter(_._2=="true").map(kxv=>{
      val (computeStatisticId, flag, stepConfMap) = kxv

      val filteredStepsMap = stepConfMap.filter{case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"}

      println("=  = " * 10 + "[myapp configuration] compute_computesConfMap_active for computeStatisticId = " + computeStatisticId + ", compute_computesConfMap_active = ")
      //println(filteredStepsMap.mkString("[", ",", "]"))
      filteredStepsMap.foreach{case (k, vMap) =>
        println("- - " * 10)
        println("stepId = " + k +", stepConf = " + vMap.mkString("[", ",\n", "]"))
      }

      (computeStatisticId, filteredStepsMap.toSeq.sortWith(_._1 < _._1))
    }).toMap

    //TODO: 新封装
    val compute_computesRule_active = compute_computesConfTuple.filter(_._2=="true").map(kxv=>{
      val (computeStatisticId, flag, stepConfMap) = kxv

//      val filteredStepsMap = stepConfMap.filter{case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"}
//
//      println("=  = " * 10 + "[myapp configuration] compute_computesConfMap_active for computeStatisticId = " + computeStatisticId + ", compute_computesConfMap_active = ")
//      //println(filteredStepsMap.mkString("[", ",", "]"))
//      filteredStepsMap.foreach{case (k, vMap) =>
//        println("- - " * 10)
//        println("stepId = " + k +", stepConf = " + vMap.mkString("[", ",\n", "]"))
//      }

      val rules = stepConfMap.filter{
        case (k, vMap)=>vMap.getOrElse("step.enabled", "false")=="true"
      }.map{case (ruleId, ruleConf) =>
        //val ruleType = conf("step.type")
        val ruleMethod = ruleConf("step.method")
        // val ruleEnabled = conf("step.enabled")
        val clz_name = appsPropMap(appId).getOrElse("class", "com.xuetangx.streaming.StreamingRDDRule")
        val rule = Class.forName(clz_name).newInstance().asInstanceOf[StreamingRDDRule]

        val cacheConf = ruleConf.get("cacheId") match {
          case Some(cacheId) if cacheId.trim.nonEmpty => cachesPropMap(cacheId.trim)
          case _ => null
        }

        // 设置输出插件类和配置
        val outputConf = ruleConf.get("output.dataInterfaceId") match {
          case Some(di_output_id) if di_output_id.trim.nonEmpty =>
            di_confs_with_ds_conf(di_output_id.trim)
          case _ => null
        }

//        val outputRule_clz_name = ruleConf.getOrElse("output.class", "com.xuetangx.streaming.rules.ConsoleOutputRule")
//        val outputRule = Class.forName(outputRule_clz_name).newInstance().asInstanceOf[StreamingRDDRule]
//        outputRule.setOutputConf(outputConf)
//        rule.setOutputRule(outputRule = outputRule)
        rule.init(ruleId, conf = ruleConf, cacheConf = cacheConf, outputConf = outputConf)
        rule
      }.toSeq.sortWith(_.ruleId < _.ruleId)

      (computeStatisticId, rules)
    }).toMap


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

//    appsPropMap(appId).get("spark.streaming.concurrentJobs") match {
//      case Some(x) if x.nonEmpty => sparkConf.set("spark.streaming.concurrentJobs", x.trim)
//      case _ =>
//    }

    val sc = new SparkContext(sparkConf)

    // TODO: 使用 broadcast 优化外部缓存
    // 测试方法1：该方法有问题, 每个 task 反序列后，需要反复进行 broadcast操作，非常慢
    //cache_broadcasts: Map[String, Broadcast[Map[String, Map[String, String]]]
//    val cache_broadcasts =
//      cachesPropMap.filter{
//        case (cacheId, cacheConfMap) =>
//          cacheConfMap.getOrElse("broadcast.enabled", "false").toBoolean
//      }.map{case (cacheId, cacheConfMap) =>
//        val ds = JdbcPool.getPool(cacheConfMap)
//        val keyName = cacheConfMap("keyName")
//        val selectKeyNames = cacheConfMap.get("cache.keyName.list") match {
//          case Some(x) if x.nonEmpty => x
//          case _ => "*"
//        }
//        val tableName = cacheConfMap("tableName")
//        val cacheSql = "select " + keyName + ", " + selectKeyNames + " from " + tableName
//        (cacheId, sc.broadcast(JdbcUtils.getQueryResultAsMap2(cacheSql, keyName, ds)))
//      }

    // 测试方法2：该方法有问题, 每个 task 反序列后，需要反复进行 broadcast操作，非常慢
//    val cache =
//      cachesPropMap.filter{
//        case (cacheId, cacheConfMap) =>
//          cacheConfMap.getOrElse("broadcast.enabled", "false").toBoolean
//      }.map{case (cacheId, cacheConfMap) =>
//        val ds = JdbcPool.getPool(cacheConfMap)
//        val keyName = cacheConfMap("keyName")
//        val selectKeyNames = cacheConfMap.get("cache.keyName.list") match {
//          case Some(x) if x.nonEmpty => x
//          case _ => "*"
//        }
//        val tableName = cacheConfMap("tableName")
//        val cacheSql = "select " + keyName + ", " + selectKeyNames + " from " + tableName
//        (cacheId, JdbcUtils.getQueryResultAsMap2(cacheSql, keyName, ds))
//      }
//
//    val cache_broadcast = sc.broadcast(cache)

    //Note: broacast 不能这么使用，原因：executor收到 task后的rule后，反序列化后的Rule 示例中的 broadcast 和 driver端的broadcast不是同一个对象，
    // 导致driver每次都会重新broacast
//    preparesRule_active.filter(rule => {
//      rule.cacheConf.getOrElse("broadcast.enabled", "false").toBoolean
//    }).foreach(rule => {
//      val cacheId = rule.cacheConf("cache.id")
//      rule.setCacheBroadcast(cache_broadcasts(cacheId))
//    })

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
    val clz = appsPropMap(appId).getOrElse("class", "com.xuetangx.streaming.StreamingAppNew")
    val instance = Class.forName(clz).newInstance().asInstanceOf[StreamingAppNew]
    //  流数据处理流程
    //instance.process(stream, ssc, sqlc, preparesRule_active, compute_preparesRule_active, compute_computesRule_active, cache_broadcasts)
    //instance.process(stream, ssc, sqlc, preparesRule_active, compute_preparesRule_active, compute_computesRule_active, cache_broadcast)

    instance.process(stream, ssc, sqlc, preparesRule_active, compute_preparesRule_active, compute_computesRule_active)

    //测试恢复
    //instance.test(stream, ssc, sqlc, preparesRule_active)
    //instance.test2(stream, ssc, sqlc, preparesRule_active)

    ssc.start()
    ssc.awaitTermination()
  }


}


/** 20151009 实现批次过滤
  *
  */
class StreamingAppNew extends Serializable with Logging {

  /**
   * 测试 rdd + df unpersist
   * @param dStream
   * @param ssc
   * @param sqlc
   * @param preparesRules_active
   */
  def test(dStream: DStream[String],
           ssc: StreamingContext, //For 最近上一批次和本批次排重
           sqlc: SQLContext,
           preparesRules_active: Seq[StreamingRDDRule]): Unit ={

    val format_rules = preparesRules_active.filter(_.conf.getOrElse("step.type", "") == "format")
    val dStream2 = dStream.transform(rdd=>{
      val rddJson =
        if (format_rules.isEmpty) {
          println("- - " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream.transform] found no format_class")
          rdd
        } else {
          val rule = format_rules.head
          println("- - " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream.transform] found format_class = " + rule.conf.getOrElse("class", "no format.class"))
          // 约定format规则每个数据结构只有一个
          rule.format(rdd)
        }

      //判断数据是否为空，不为空执行一些操作
      rddJson.persist()  //此处 persist 确保，format_class 插件类 不重复结算
      val df = sqlc.jsonRDD(rddJson)
      df.printSchema()
      val res =
        if (df.schema.fieldNames.length > 0) {
          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream.transform] found df has data")
          rddJson
        } else {
          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream.transform] found empty df")
          sqlc.sparkContext.emptyRDD[String]
        }
      res
    })

    val selectExprClause = "time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id"
    dStream2.foreachRDD(rdd=>{
      //判断数据是否为空
      rdd.persist()  //此处 persist 确保，format_class 插件类 不重复计算
      val df = sqlc.jsonRDD(rdd)
      if (df.schema.fieldNames.length > 0) {
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] df has data")
        df.printSchema()
        val df2 = df.selectExpr(selectExprClause.split(","): _*)
        df2.persist()
        df2.printSchema()
        val df3 = df2.groupBy("platform").agg(count("user_id"))
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] df3.show()")
        df3.show()
        val rdd2 = df3.toJSON

        val result_cnt = rdd2.count()
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] result_cnt = " + result_cnt)
      } else {
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] found empty df")
      }
      //rdd.unpersist()  //测试点1 不做 rdd.unpersist
      // df.unpersist()  //测试点2：不做 df.unpersist
    })
  }



  /**
   * 测试 rdd + df unpersist
   * @param dStream
   * @param ssc
   * @param sqlc
   * @param preparesRules_active
   */
  def test2(dStream: DStream[String],
           ssc: StreamingContext, //For 最近上一批次和本批次排重
           sqlc: SQLContext,
           preparesRules_active: Seq[StreamingRDDRule]): Unit ={

    val rdd_cache_map = scala.collection.mutable.Map[String, RDD[String]]()
    val df_cache_map = scala.collection.mutable.Map[String, DataFrame]()

    val format_rules = preparesRules_active.filter(_.conf.getOrElse("step.type", "") == "format")
    val dStream2 = dStream.transform(rdd=>{
      val rddJson =
        if (format_rules.isEmpty) {
          println("- - " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream.transform] found no format_class")
          rdd
        } else {
          val rule = format_rules.head
          println("- - " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream.transform] found format_class = " + rule.conf.getOrElse("class", "no format.class"))
          // 约定format规则每个数据结构只有一个
          rule.format(rdd)
        }

      //判断数据是否为空，不为空执行一些操作
      rddJson.persist()  //此处 persist 确保，format_class 插件类 不重复结算
      rdd_cache_map.put("rdd_after_format" + "_" + System.currentTimeMillis(), rddJson)
      val df = sqlc.jsonRDD(rddJson)
      df.printSchema()
      val res =
        if (df.schema.fieldNames.length > 0) {
          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream.transform] found df has data")
          rddJson
        } else {
          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream.transform] found empty df")
          sqlc.sparkContext.emptyRDD[String]
        }
      res
    })

    val selectExprClause = "time, uuid, user_id, event_type, host, platform, origin_referer, spam, course_id"
    dStream2.foreachRDD(rdd=>{
      //判断数据是否为空
      rdd.persist()  //此处 persist 确保，format_class 插件类 不重复计算
      rdd_cache_map.put("rdd_begin_foreachrdd" + "_" + System.currentTimeMillis(), rdd)
      val df = sqlc.jsonRDD(rdd)
      if (df.schema.fieldNames.length > 0) {
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] df has data")
        df.printSchema()
        val df2 = df.selectExpr(selectExprClause.split(","): _*)
        df2.persist()
        df_cache_map.put("df_selected" + "_" + System.currentTimeMillis(), df2)

        df2.printSchema()
        val df3 = df2.groupBy("platform").agg(count("user_id"))
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] df3.show()")
        df3.show()
        val rdd2 = df3.toJSON

        val result_cnt = rdd2.count()
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] result_cnt = " + result_cnt)
      } else {
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] found empty df")
      }
      //rdd.unpersist()  //测试点1 不做 rdd.unpersist
      // df.unpersist()  //测试点2：不做 df.unpersist

      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] found rdd_cache_map = " + rdd_cache_map.keys.mkString("[", ",", "]"))
      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp test.dStream2.foreachRDD] found df_cache_map = " + df_cache_map.keys.mkString("[", ",", "]"))

      rdd_cache_map.values.foreach(_.unpersist())
      df_cache_map.values.foreach(_.unpersist())
    })
  }

  /** 定义流数据处理流程
    * 1 文本日志到DataFrame的转换，如果日志是json字符串，直接转换(可优化的点1，可配置schema)；如果csv等，配置字段名先转换为json形式，再处理
    * 2
    * 3
    * @param dStream
    */
  def process(dStream: DStream[String],
              ssc: StreamingContext, //For 最近上一批次和本批次排重
              sqlc: SQLContext,
              preparesRules_active: Seq[StreamingRDDRule],
              compute_preparesRules_active: Map[String, Seq[StreamingRDDRule]],
              compute_computesRules_active: Map[String, Seq[StreamingRDDRule]]
              //,
              //cache_broadcasts: Map[String, Broadcast[Map[String, Map[String, String]]]],
              //cache_broadcast: Broadcast[Map[String, Map[String, Map[String, String]]]]
                     ): Unit = {

    val rdd_cache_map = scala.collection.mutable.Map[String, RDD[String]]()
    val df_cache_map = scala.collection.mutable.Map[String, DataFrame]()

    //TODO: broadcast
    // 测试方法1：该方法有问题, 每个 task 反序列后，需要反复进行 broadcast操作，非常慢
//    def fun_get_broadcast_value(cacheId: String): Map[String, Map[String, String]] ={
//      if (cache_broadcasts.contains(cacheId)){
//        cache_broadcasts(cacheId).value
//      } else {
//        //Map[String, Map[String, String]]()
//        null
//      }
//    }

    //测试方法2: 该方法有问题, 每个 task 反序列后，需要反复进行 broadcast操作，非常慢
//    def fun_get_broadcast: Broadcast[Map[String, Map[String, Map[String, String]]]] = cache_broadcast



    // 准备阶段处理
    // Note: 准备阶段不支持 batchDeduplicate 步骤
    //  准备阶段处理1
    // Note: dStream_after_prepares 中的 rdd 是json形式
    val dStream_after_prepares =
      if (preparesRules_active.isEmpty) {
        //准备阶段没有配置步骤
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] found no active preparesRules")
        dStream
      } else {
        //准备阶段配置了执行步骤
        println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] found active preparesRules")
        dStream.transform(rdd => {
          val format_rules = preparesRules_active.filter(_.conf.getOrElse("step.type", "") == "format")

          val rddJson =
            if (format_rules.isEmpty) rdd
            else {
              val format_rule = format_rules.head
              // 约定format规则每个数据结构只有一个
              format_rule.format(rdd)
            }

          //优化 df 的生成
          rddJson.persist()
          rdd_cache_map.put("rdd_after_format" + "_" + System.currentTimeMillis(), rddJson)
          val df = sqlc.jsonRDD(rddJson)
          val schema = df.schema

          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] df.schema.length = " + df.schema.length)
          df.printSchema()

          if (schema.fieldNames.isEmpty){
            //rddJson
            sqlc.sparkContext.emptyRDD[String]
          } else {
            // 初始化准备阶段每个步骤输出结果
            var res_any: Any = df
            var res_schema: StructType = schema

            // 遍历准备阶段准备步骤
            preparesRules_active.filterNot(_.conf.getOrElse("step.type", "") == "format").foreach {
              case rdd_rule =>
                val rule_step = "prepares.id[" + rdd_rule.conf("prepares.id") + "]-prepare.id[" + rdd_rule.ruleId + "]"
                println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing rule_step " + rule_step)
                println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] res_any is RDD or not = " + res_any.isInstanceOf[RDD[String]] + ", is DataFrame or not = " + res_any.isInstanceOf[DataFrame] + ", prepares.step.id = " + rdd_rule.ruleId)

                val cache_id = rdd_rule.cacheConf.getOrElse("cache.id", "")

                //val res = processStep(res_any, res_schema, sqlc, rdd_rule, rule_step, cache_broadcasts.getOrElse(cache_id, null))
                //val res = processStep(res_any, res_schema, sqlc, rdd_rule, rule_step, fun_get_broadcast_value _)
                //val res = processStep(res_any, res_schema, sqlc, rdd_rule, rule_step, fun_get_broadcast _)
                val res = processStep(res_any, res_schema, sqlc, rdd_rule, rule_step)
                res_any = res._1
                res_schema = res._2
                res._3.foreach{case (k, vrdd) => rdd_cache_map.put(k, vrdd)}
                res._4.foreach{case (k, vdf) => df_cache_map.put(k, vdf)}
                println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] finish processing rule_step " + rule_step)
            }
            val rdd2 = res_any match {
              case x: RDD[String] => x
              case x: DataFrame => x.toJSON
            }
            rdd2
          }
        })
      }

    // 计算统计指标阶段处理
    //Note: 循环流程： 准备(过滤->增强)->批次排重+计算->输出
    //  过滤2(依赖增强1的属性)
    //  增强2(在过滤2的基础上)
    //    if(computesActiveConfTupleSeq.isEmpty) { //配置中没有配置有效的计算，不需要触发job
    //if(compute_preparesConfMap_active.isEmpty && compute_computesConfMap_active.isEmpty) { //配置中没有配置有效的计算，不需要触发job
    if (compute_computesRules_active.isEmpty) {
      //配置中没有配置有效的计算，不需要触发job
      // 如果不配置foreacheRDD 会提示：Exception in thread "main" java.lang.IllegalArgumentException: requirement failed: No output operations registered, so nothing to execute
      dStream_after_prepares.foreachRDD(rddJson => {
        logWarning("[myapp configuration] found no active computeStatistic in computeStatistics part")
        //rddJson //不执行action操作
        SparkUtils.unpersist_rdd_df(rdd_cache_map, df_cache_map)
      })
    } else {
      //存在有效的计算配置
      //TODO: 测试内存缓存
      //      try {
      //        dStream_after_prepares.persist() //Note: 不启用 cache，发现本地调试 DeDuplicateProcessor 时，看到调试debug日志总是输出两次，导致排重属性总是0，启用 cache，正常
      //      } catch {
      //        case ex: UnsupportedOperationException =>
      //          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) +
      //                  " [myapp] WARNING StreamingApp.process.computes stream has been persist after prepares")
      //        case ex => throw ex
      //      }

      // 每个有效的 computeStatistic 对应一个过滤条件
      val id2streams =
        compute_computesRules_active.keys.map(computeStatisticId => {
          if (compute_preparesRules_active.contains(computeStatisticId)) {
            val rules = compute_preparesRules_active(computeStatisticId)

            //Note: 每个 computeStatisticId 对应一些过滤条件，
            //   应该有各自的 res_any_in_computeStatistic, res_schema_in_computeStatistic, 初始化可以是准备阶段的值,
            //   但是问题是定义时不在 DStream.transform中执行，有问题，所以初始化为null
            // 输入：dStream_after_prepares
            // 输出：

            println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing computeStatisticId " + computeStatisticId)

            // 处理计算阶段流程，准备步骤 - 1先处理filter 和 enhance
            //          val computePreparesStepsConfSeq = stepsConfSeq.filterNot {
            //            case (id, confMap) => confMap("step.type") == "compute" || confMap("step.type") == "batchDeduplicate"
            //          }
            val computePreparesRules = rules.filter { case rule => rule.conf("step.type") != "batchDeduplicate" }

            // 计算每个有效统计指标集合——先执行准备步骤，再计算，最后输出
            val dStream3 =
              if (computePreparesRules.isEmpty) {
                dStream_after_prepares
              } else {
                dStream_after_prepares.transform(rddJson => {
                  var res_any_in_computeStatistic: Any = rddJson
                  var res_schema_in_computeStatistic: StructType = null

                  // 遍历计算阶段准备步骤
                  computePreparesRules.foreach {
                    case rdd_rule =>
                      val rule_step = "computeStatistic.id[" + computeStatisticId + "]-prepares.step.id[" + rdd_rule.ruleId + "]"
                      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing rule_step " + rule_step)

                      //println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] res_any is RDD or not = " + res_any_in_computeStatistic.isInstanceOf[RDD[String]] + ", is DataFrame or not = " + res_any_in_computeStatistic.isInstanceOf[DataFrame] + ", rule_step " + rule_step)

                      val cache_id = rdd_rule.cacheConf.getOrElse("cache.id", "")

                      //val res = processStep(res_any_in_computeStatistic, res_schema_in_computeStatistic, sqlc, rdd_rule, rule_step, fun_get_broadcast_value _)
                      //val res = processStep(res_any_in_computeStatistic, res_schema_in_computeStatistic, sqlc, rdd_rule, rule_step, fun_get_broadcast _)
                      val res = processStep(res_any_in_computeStatistic, res_schema_in_computeStatistic, sqlc, rdd_rule, rule_step)
                      res_any_in_computeStatistic = res._1
                      res_schema_in_computeStatistic = res._2
                      res._3.foreach{case (k, vrdd) => rdd_cache_map.put(k, vrdd)}
                      res._4.foreach{case (k, vdf) => df_cache_map.put(k, vdf)}
                      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process finish processing rule_step " + rule_step)
                  }

                  res_any_in_computeStatistic match {
                    case x: RDD[String] => x
                    case x: DataFrame => x.toJSON
                  }
                })
              }

            (computeStatisticId, dStream3)
          } else {
            (computeStatisticId, dStream_after_prepares)
          }
        })

      // 处理计算阶段流程，计算步骤 - 3 批次去重 + 计算统计指标
      id2streams.foreach {
        case (computeStatisticId, stream) =>
          val computesStepRules = compute_computesRules_active.get(computeStatisticId).get

          if (computesStepRules.isEmpty) {
            //配置中没有配置有效的计算统计指标，不需要action操作，触发job
            stream.foreachRDD(rddJson => {
              println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [Warning myapp configuration] StreamingApp.process.compute found no active computeStatistic.computes in computeStatistics part")
              //rddJson
              SparkUtils.unpersist_rdd_df(rdd_cache_map, df_cache_map)
            })
          } else {
            //存在有效的计算配置

            // 多少个 computeStatistic，多少个stream，多少个job
            // stream： DStream[String]
            stream.foreachRDD(rddJson => {
              if (SparkUtils.persist_rdd(rddJson) != null){
                rdd_cache_map.put("rdd_begin_stream_foreachRDD" + "_" + System.currentTimeMillis(), rddJson)
              }
              val df = sqlc.jsonRDD(rddJson)
              val schema = df.schema

              println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes begin action for computeStatisticId = " + computeStatisticId)
              if (schema.fieldNames.isEmpty) {
                //数据为空
                println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes found empty dataframe after computePrepare in computeStatisticId = " + computeStatisticId)
              } else {
                //rddJson.persist() // 问题： 不能与 stream.persist() 一同使用，否则报错：java.lang.UnsupportedOperationException: Cannot change storage level of an RDD after it was already assigned a level
                //数据不为空
                val compute_step_rdds =
                  computesStepRules.map {
                    //computesStepConfSeq.par.foreach {
                    case rdd_rule: StreamingRDDRule =>
                      val rule_step = "computeStatistic.id[" + computeStatisticId + "]-computes.step.id[" + rdd_rule.ruleId + "]"
                      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process start processing rule_step " + rule_step)

                      val cache_id = rdd_rule.cacheConf.getOrElse("cache.id", "")

                      // compute 步骤输出的结果是 每个 computeStatistic.computes的结果
                      //val res = processStep(df, schema, sqlc, rdd_rule, rule_step, cache_broadcasts.getOrElse(cache_id, null))
                      //val res = processStep(df, schema, sqlc, rdd_rule, rule_step, fun_get_broadcast_value _)
                      //val res = processStep(df, schema, sqlc, rdd_rule, rule_step, fun_get_broadcast _)
                      val res = processStep(df, schema, sqlc, rdd_rule, rule_step)
                      res._3.foreach{case (k, vrdd) => rdd_cache_map.put(k, vrdd)}
                      res._4.foreach{case (k, vdf) => df_cache_map.put(k, vdf)}
                      println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process finish processing rule_step " + rule_step)
                      res._1.asInstanceOf[RDD[String]]
                  }

                // val compute_step_rdds_nonEmpty = compute_step_rdds.filterNot(_.isEmpty()) // 不使用 rdd.isEmpty 的原因，避免触发job
                val compute_step_rdds_nonEmpty = compute_step_rdds.filter(_.partitions.nonEmpty)
                if (compute_step_rdds_nonEmpty.nonEmpty) {
                  val result = compute_step_rdds_nonEmpty.reduce(_ union _)
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes do compute action for computeStatisticId = " + computeStatisticId)

                  result.count() //触发job执行
                } else {
                  println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process.computes found empty result, need no compute action for computeStatisticId = " + computeStatisticId)
                }
              }
              rddJson.unpersist()
              SparkUtils.unpersist_rdd_df(rdd_cache_map, df_cache_map)
            })
          }
          println("= = " * 4 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.process finish processing computeStatistics in computeStatisticId " + computeStatisticId)
      }
    }

  }

  def processStep(res_any: Any,
                  res_schema: StructType,
                  sqlc: SQLContext,
                  rdd_rule: StreamingRDDRule,
                  rule_step: String = null
                  //,
                  //cache_broadcast: Broadcast[Map[String, Map[String, String]]]
                  //fun_get_broadcast_value: (String) => Map[String, Map[String, String]],
                  //fun_get_broadcast: () => Broadcast[Map[String, Map[String, Map[String, String]]]]
                         ): (Any, StructType, scala.collection.mutable.Map[String, RDD[String]], scala.collection.mutable.Map[String, DataFrame]) = {

    val rdd_cache_map = scala.collection.mutable.Map[String, RDD[String]]()
    val df_cache_map = scala.collection.mutable.Map[String, DataFrame]()

    val confMap = rdd_rule.conf

    val stepType = confMap("step.type")
    val stepMethod = confMap("step.method")
    println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.processStep."+ stepType +" start preocessing rule_step = " + rule_step)

    stepType match {
      case "filter" | "enhance" =>
        stepMethod match {
          case "spark-sql" => //spark-sql方式处理后，输出DataFrame
            val df = res_any match {
              case x: RDD[String] =>
                if (SparkUtils.persist_rdd(x) != null) {
                  rdd_cache_map.put(rule_step + "_" + System.currentTimeMillis(), x)
                }
                sqlc.jsonRDD(x, res_schema)
              case x: DataFrame =>
                x
            }

            println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares] df.schema = ")
            df.printSchema()

            if (df.schema.fieldNames.nonEmpty) {
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

              println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares data after stepPhase = " + rule_step +"], df2.rdd.length = " + df2.rdd.partitions.length + ", df2 = ")
              // TODO: 测试 schmea
              df2.printSchema()

              (df2, df2.schema, rdd_cache_map, df_cache_map)
            } else { // 空的 DataFrame
              print("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares found empty DataFrame] in stepPhase = " + rule_step)
              (df, df.schema, rdd_cache_map, df_cache_map)
            }

          case "plugin" => //插件类处理后，结构可能变化，

            val rdd2 = res_any match {
              case x: RDD[String] =>
                //rdd_rule.process(x, fun_get_broadcast_value)
                //rdd_rule.process(x, fun_get_broadcast)
                rdd_rule.process(x)
              case x: DataFrame =>
                //rdd_rule.process(x.toJSON, fun_get_broadcast_value)
                //rdd_rule.process(x.toJSON, fun_get_broadcast)
                rdd_rule.process(x.toJSON)
            }
            println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.prepares data after stepPhase = " + rule_step +"], rdd2.partitions.length = " + rdd2.partitions.length)
            (rdd2, null, rdd_cache_map, df_cache_map)
        }

      case "compute" =>
        // 计算统计指标的逻辑
        stepMethod match {
          case "spark-sql" => //spark-sql计算统计指标
            val df = res_any match {
              case x: RDD[String] =>
                println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] stepPhase = " + rule_step + ", res_schema null or not = " + (res_schema == null))
                if (SparkUtils.persist_rdd(x) != null){
                  rdd_cache_map.put(rule_step + "_" + System.currentTimeMillis(), x)
                }
                sqlc.jsonRDD(x, res_schema)
              case x: DataFrame => x
            }
            println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] data before stepPhase = " + rule_step +", df.rdd.length = " + df.rdd.partitions.length + ", df = ")
            df.printSchema()

            val result =
              if (df.schema.fieldNames.isEmpty) {
                println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] found empty dataFrame before compute rule_step = " + rule_step)
                (sqlc.sparkContext.emptyRDD, null, rdd_cache_map, df_cache_map)
              } else {
                println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] data before stepPhase = " + rule_step + ", df.schema.fieldNames.length = " + df.schema.fieldNames.length)

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

                println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute data after select and where, stepPhase = " + rule_step + "], df2.schema.fieldNames.length = " + df2.schema.fieldNames.length + ", df2 = ")
                df2.printSchema()

                // TODO: 此处是由 df.schema.fieldNames.nonEmpty 的 df 过滤生成， 新的 schema.fieldNames.nonEmpty 一定为 true
                // TODO: 优化点，如果能判断没有数据，可以不用处理后面的数据

//                if (!df2.rdd.isEmpty()) {
                  df2.persist()
                df_cache_map.put(rule_step + "_" + "before_group_by" + "_" + System.currentTimeMillis(), df2)

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
                  val output_plugin = rdd_rule.outputRule

                  println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp  StreamingApp.processStep.compute ] begin computing with targetKeysList = " + targetKeysList)

                  val staticKeyLabelExcludes = confMap.get("statistic.keyLabel.excludes") match {
                    case Some(x) if x.nonEmpty => x.split(",").map(_.trim)
                    case _ => Array[String]()
                  }
                  val DUMMY_SPLIT_PLACEHOLDER = "DUMMY_SPLIT_PLACEHOLDER"

                  val resultSeq_output =
                  // 根据统计维度列表，遍历每个统计维度；对每个统计维度进行 df + groupBy + count
                    targetKeysList.split(",").map(targetKeys => {
                      println("= = " * 10 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp  StreamingApp.processStep.compute ] begin computing with targetKeys = " + targetKeys)

                      val keyDtypeVtypeArr = (targetKeys + "#" + DUMMY_SPLIT_PLACEHOLDER).split("#").dropRight(1).map(_.trim)
                      val keyLevelListStr = keyDtypeVtypeArr(0)
                      val groupByKeyArr = keyLevelListStr.split(":").map(k => {
                        if (k == DEFAULT_GROUPBY_NONE_VALUE) GROUPBY_NONE_VALUE else statisticKeyMap(k)
                      })
                      val groupByKeyArr_without_none_value = groupByKeyArr.filterNot(_ == GROUPBY_NONE_VALUE)

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
                      val keyValueType = if (keyDtypeVtypeArr.length > 2) keyDtypeVtypeArr(2) else confMap("value.type")
                      val valueCycle = if (keyDtypeVtypeArr.length > 3) keyDtypeVtypeArr(3) else confMap("value.cycle")

                      assert(keyDataType.nonEmpty, "invalid configuration in " + STATSTIC_KEYS_INFO_KEY + ", data.type of " + keyLevelListStr + " is empty")
                      assert(keyValueType.nonEmpty, "invalid configuration in " + STATSTIC_KEYS_INFO_KEY + ", value.type of " + keyLevelListStr + " is empty")
                      assert(valueCycle.nonEmpty, "invalid configuration in " + STATSTIC_KEYS_INFO_KEY + ", value.cycle of " + keyLevelListStr + " is empty")

                      // pre_output 需要的配置信息: 统计指标维度，data_type(指标关联实体标识), value_type(指标取值含义)
                      val outputConfMap = rdd_rule.outputConf ++
                              Map[String, String](
                                "origin.statistic.key" -> groupByKeyArr.mkString(","),
                                "origin.statistic.keyLevel" -> keyLevelListStr,
                                "statistic.key" -> (if (keyLevelList2.nonEmpty) keyLevelList2.mkString(",") else "L0"),
                                "statistic.keyName" -> (if (groupByKeyArr2.nonEmpty) groupByKeyArr2.mkString(",") else "NONE"),
                                "statistic.data_type" -> keyDataType,
                                "statistic.value_type" -> keyValueType,
                                "statistic.cycle" -> valueCycle
                              )
                      output_plugin.setOutputConf(outputConfMap)

                      val rdd_stat_arr =
                        aggregateKeyMethodList.map {
                          case (key, method) =>
                            println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute] using spark sql dataframe, df2.printSchema() = ")
                            //df2.printSchema()

                            val df_stat =
                              method match {
                                case "count" =>
                                  //println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute using count before union] df2.rdd.partitions.length = " + df2.rdd.partitions.length + ", groupByKeyArr = " + groupByKeyArr.mkString("[", ",", "]"))
                                  if (groupByKeyArr_without_none_value.isEmpty) {
                                    // groupByKey为 None，取当前批次数据的记录数
                                    df2.agg(count(key).alias("value"))
                                  } else {
                                    df2.groupBy(groupByKeyArr_without_none_value(0), groupByKeyArr_without_none_value.drop(1): _*).agg(count(key).alias("value"))
                                  }
                                case "countDistinct" =>
                                  println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute using countDistinct before union] df2.rdd.partitions.length = " + df2.rdd.partitions.length + ", groupByKeyArr = " + groupByKeyArr.mkString("[", ",", "]"))

                                  if (groupByKeyArr_without_none_value.isEmpty) {
                                    // groupByKey为 None，取当前批次数据的记录数
                                    df2.agg(countDistinct(key).alias("value"))
                                  } else {
                                    df2.groupBy(groupByKeyArr_without_none_value(0), groupByKeyArr_without_none_value.drop(1): _*).agg(countDistinct(key).alias("value"))
                                  }
                              }
                            println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute.df_stat] after compute , groupByKeyArr = " + groupByKeyArr.mkString("[", ",", "]") + ", df_stat = ")
                            val rdd_stat = df_stat.toJSON
                            output_plugin.output(rdd_stat)
                        }
                      val rdd_output = rdd_stat_arr.reduce(_ union _)
                      //触发job的action操作
                      // Note: union 统计结果的 rdd，可以减少 job 的数量，增加 partition 数量，增加任务并行
                      rdd_output
                    })

                  (resultSeq_output.reduce(_ union _), null, rdd_cache_map, df_cache_map)
//                } else {
//                  println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp StreamingApp.processStep.compute ] found empty dataFrame after rule_step = " + rule_step)
//                  //(null, null)
//                  (sqlc.sparkContext.emptyRDD, null)
//                }
              }
            result

          case "plugin" => //插件类计算统计指标
            //TODO: 支持插件类方式计算统计指标

            val rdd_stat = res_any match {
              case x: RDD[String] => rdd_rule.compute(x)
              case x: DataFrame =>
                //TODO: 支持更好的控制
                rdd_rule.compute(x.toJSON)
            }

            //触发job的action操作
            val outputPlugin = rdd_rule.outputRule
            val rdd_output = outputPlugin.output(rdd_stat)

            //TODO: 支持插件类方式计算统计指标
            //throw new Exception("does not support to compute statics using plugin class")
            (rdd_output, null, rdd_cache_map, df_cache_map)
        }
      //println("= = " * 8 + DateFormatUtils.dateMs2Str(System.currentTimeMillis()) + " [myapp] StreamingApp.processStep finished preocessing rule_step = " + rule_step)
    }
  }
}
