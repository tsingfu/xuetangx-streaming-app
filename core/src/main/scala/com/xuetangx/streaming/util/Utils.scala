package com.xuetangx.streaming.util

import scala.collection.JavaConversions._
import scala.collection.immutable.Map
import scala.xml.NodeSeq

/**
 * Created by tsingfu on 15/10/6.
 */
object Utils {

  def getClassLoader : ClassLoader = getClass.getClassLoader

  /** Returns the system properties map that is thread-safe to iterator over. It gets the
    * properties which have been set explicitly, as well as those for which only a default value
    * has been defined. */
  def getSystemProperties: Map[String, String] = {
    val sysProps = for (key <- System.getProperties.stringPropertyNames()) yield
    (key, System.getProperty(key))

    sysProps.toMap
  }


  /**解析类似如下结构的xml转换为map
    * 示例：
    *{{{
    <prepares id="1" interfaceId="1" phase="2">
        <prepare id="1" type="filter" method="spark-sql" enabled="true">
            <properties>
                <property>
                    <name>selectExprClause</name>
                    <value></value>
                    <description>设置需要用到的字段名，流程：读入数据后，转换为spark-SQL的DataFrame，然后进行 selectExp，仅保留需要的数据</description>
                </property>
                <property>
                    <name>whereClause</name>
                    <value></value>
                    <description></description>
                </property>
            </properties>
        </prepare>
        <prepare id="2" type="filter" method="plugin" enabled="false">
            <properties>
                <property>
                    <name>class</name>
                    <value>com.xuetangx.streaming.filtersBeforeEnhance.Filter1</value>
                    <description></description>
                </property>
            </properties>
        </prepare>
        <prepare id="3" type="enhance" method="plugin" enabled="true">
            <properties>
                <property>
                    <name>class</name>
                    <value>com.xuetangx.streaming.enhanceBeforeFilter.Enhance1</value>
                    <description></description>
                </property>
                <property>
                    <name>cacheManager</name>
                    <value>JdbcCacheManager</value>
                    <description></description>
                </property>
            </properties>
        </prepare>
        <prepare id="4" type="enhance" method="spark-sql" enabled="true">
            <properties>
                <property>
                    <name>selectExprClause</name>
                    <value></value>
                    <description></description>
                </property>
            </properties>
        </prepare>
    </prepares>
    }}}
    *
    * @param nodeSeq
    * @param nodeName
    * @param keyName
    * @return
    */
  def parseProperties2Map(nodeSeq: NodeSeq,
                          nodeName: String,
                          keyName: String): Map[String, Map[String, String]] = {

    val outerAttrMap = nodeSeq.head.attributes.asAttrMap.map{case (k, v) => (nodeSeq.head.label+"."+k, v)}

    val nodes = nodeSeq \\ nodeName
    val res = for(node<-nodes) yield {
      val innerAttrMap = node.attributes.asAttrMap.map{case (k, v) => (node.label+"."+k, v)}

      val props = node \ "properties" \ "property"
      val names = props \ "name"
      val values = props \ "value"
      val nameValueSeq = names.map(_.text).zip(values.map(_.text)) ++ innerAttrMap ++ outerAttrMap
      val propsMap = (for (nameValue<- nameValueSeq) yield (nameValue._1, nameValue._2)).toMap
      // println(propsMap.mkString("[", ",", "]"))
      (propsMap(keyName), propsMap)
    }
    //    (res.toMap, outerAttrMap)
    //    (outerAttrMap, res.toMap)
    res.toMap
  }

  def strip(str: String, trimStr: String): String ={
    str.stripPrefix(trimStr).stripSuffix(trimStr)
  }



  /**
   * Returns a human-readable string representing a duration such as "35ms"
   */
  def msDurationToString(ms: Long): String = {
    val second = 1000
    val minute = 60 * second
    val hour = 60 * minute

    ms match {
      case t if t < second =>
        "%d ms".format(t)
      case t if t < minute =>
        "%.1f s".format(t.toFloat / second)
      case t if t < hour =>
        "%.1f m".format(t.toFloat / minute)
      case t =>
        "%.2f h".format(t.toFloat / hour)
    }
  }
}
