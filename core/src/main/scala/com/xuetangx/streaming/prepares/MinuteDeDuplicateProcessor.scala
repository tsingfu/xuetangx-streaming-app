package com.xuetangx.streaming.prepares

/**
 * Created by tsingfu on 15/11/6.
 */
/*
class MinuteDeDuplicateProcessor  extends DeDuplicateProcessor {

  /**
   * 获取用于排重的id，存储mongo时使用的 collection 名
   * @param jValue
   * @param deDuplicateTimeKey
   * @param prefix
   * @return
   */
  override def getCollectionName(jValue: JValue, deDuplicateTimeKey: String, prefix: String): String = {
    val deDuplicateTimeValue = Utils.strip(compact(jValue \ deDuplicateTimeKey), "\"")
    // println("= = " * 20 + "[myapp debug] mongoCollectionValue = " + deDuplicateTimeValue + ", mongoCollectionKey = " + deDuplicateTimeKey)
    // 对 mongo.collection.key 指定的时间字段取值(格式 yyyy-MM-dd HH:mm:ss)进行加工， 然后返回 yyyyMMdd_HHmm
    val deDuplicateTimeValue2 = deDuplicateTimeValue.replace("-", "").replace(" ", "_").replace(":", "")
    prefix + deDuplicateTimeValue2.substring(0, 13)
  }
}
*/
