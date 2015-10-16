package com.xuetangx.streaming.prepares

import com.xuetangx.streaming.StreamingProcessor
import com.xuetangx.streaming.util.Utils
import org.apache.spark.rdd.RDD
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Created by tsingfu on 15/10/15.
 */
class EnhancePlatformProcessor extends StreamingProcessor {

  /**处理过滤和属性增强(取值更新，增减字段等)
    *
    * @param rdd
    * @param confMap
    * @return
    */
  override def process(rdd: RDD[String],
              confMap: Map[String, String],
              cacheConfMap: Map[String, String] = null,
              dataSourceConfMap: Map[String, String] = null): RDD[String] = {

    val agentKeyName = "agent"

    val platformKeyName = "platform"

    rdd.map(jsonStr=>{
      println("= = " * 10 + "[myapp EnhanceTimeProcessor.process]" + jsonStr)
      // json4s 解析json字符串
      val jValue = parse(jsonStr)

      //获取agent字段取值
      val agent_old = Utils.strip(compact(jValue \ agentKeyName), "\"") //发现字符串含有引号
      //        println("= = " * 10 +"E[DEBUG] time_old.length = " + time_old.length +", is startWith(引号)) "+ time_old.startsWith("\"") +", time_old=" + time_old)

      val agent_new = get_platform(agent_old)

      // 更新agent字段
//      val jsonStr_adding =  "{\"" + agentKeyName +" \" : \"" + agent_new +"\"}"
//      val jValue_new = jValue.merge(parse(jsonStr_adding))

      val jValue_new  = jValue.transformField{
        case JField(`agentKeyName`, _) => (platformKeyName, JString(agent_new))
      }

      val json_result = compact(jValue_new)
      println("= = " * 10 +"[myapp EnhancePlatformProcessor.process] json_result = " + json_result)
      json_result
    })
  }


  /**
   * 由agent获取平台
   * @param agent
   * @return
   */
  def get_platform(agent: String): String = {
    if (agent != null && agent.startsWith("xue tang zai xian android/")){
      "android"
    } else if (agent != null && agent.startsWith("xue tang zai xian androidTV/")) {
      "androidTV"
    } else if (agent != null && (agent.startsWith("xue tang zai xian IOS/") || agent.startsWith("xuetang IOS/"))) {
      "iPhone"
    } else if (agent != null && agent.startsWith("xuetangX-iPad IOS/")) {
      "iPad"
    } else {
      "web"
    }
  }

}
