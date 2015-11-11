package com.xuetangx.streaming.output.elasticsearch

import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.settings.{ImmutableSettings, Settings}
import org.elasticsearch.common.transport.InetSocketTransportAddress

/**
 * Created by tsingfu on 15/10/12.
 */
object ESUtils {

  // id = serverPort.list + "#" + cluster.name
  val esPool = scala.collection.mutable.Map[String, Client]()

  def getClient(confMap: Map[String, String]): Client = synchronized {
    val esClusterName = confMap("cluster.name")
    val esServerPortList = confMap("serverPort.list")
    val id = esServerPortList + "#" + esClusterName

    esPool.getOrElseUpdate(id, {
      val esServerPortStrArr = esServerPortList.split(",").map(_.trim)

      val settings: Settings = ImmutableSettings.settingsBuilder()
              // 指定集群名称
              .put("cluster.name", esClusterName)
              // 探测集群中机器状态
              .put("client.transport.sniff", true).build()

      val transportClient = new TransportClient(settings)

      esServerPortStrArr.foreach(serverPortStr=>{
        val serverPort = serverPortStr.split(":").map(_.trim)
        assert(serverPort.nonEmpty, "found invalid serverPort.list")
        val esHost = serverPort(0)

        val esPort = if (serverPort.length == 2) serverPort(1) else "9300"
        transportClient.addTransportAddress(new InetSocketTransportAddress(esHost, esPort.toInt))
      })

      val client: Client = transportClient
      client
    })
  }
}
