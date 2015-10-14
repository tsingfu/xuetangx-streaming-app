package com.xuetangx.streaming.monitor

/**
 * Created by tsingfu on 15/10/13.
 */
object MConsolePrinter {
  def output(msg: String,
             tag: String = null,
             prefix: String = null): Unit = {

    println(prefix + " [" + tag + "] " + msg)
  }
}
