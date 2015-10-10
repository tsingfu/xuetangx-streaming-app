package com.xuetangx.streaming

import java.util.concurrent.ConcurrentHashMap

import com.xuetangx.streaming.util.Utils

import scala.collection.JavaConverters._

/**
 * Created by tsingfu on 15/10/6.
 */
class AppConf(loadDefaults: Boolean) extends Cloneable with Logging with Serializable{

  def this() = this(true)

  private val settings = new ConcurrentHashMap[String, String]()

  if (loadDefaults) {
    // Load any spark.* system properties
//    for ((key, value) <- Utils.getSystemProperties if key.startsWith("spark.")) {
    for ((key, value) <- Utils.getSystemProperties if key.startsWith("streaming.")) {
      set(key, value)
    }
  }


  /** Set a configuration variable. */
  def set(key: String, value: String): AppConf = {
    if (key == null) {
      throw new NullPointerException("null key")
    }
    if (value == null) {
      throw new NullPointerException("null value for " + key)
    }
    settings.put(key, value)
    this
  }

  /** Set multiple parameters together */
  def setAll(settings: Traversable[(String, String)]): AppConf = {
    settings.foreach { case (k, v) => set(k, v) }
    this
  }

  /** Set a parameter if it isn't already configured */
  def setIfMissing(key: String, value: String): AppConf = {
    if (settings.putIfAbsent(key, value) == null) {
//      logDeprecationWarning(key)
    }
    this
  }

  /** Remove a parameter from the configuration */
  def remove(key: String): AppConf = {
    settings.remove(key)
    this
  }

  /** Get a parameter; throws a NoSuchElementException if it's not set */
  def get(key: String): String = {
    getOption(key).getOrElse(throw new NoSuchElementException(key))
  }

  /** Get a parameter, falling back to a default if not set */
  def get(key: String, defaultValue: String): String = {
    getOption(key).getOrElse(defaultValue)
  }

  /** Get a parameter as an Option */
  def getOption(key: String): Option[String] = {
    Option(settings.get(key))
  }

  /** Get all parameters as a list of pairs */
  def getAll: Array[(String, String)] = {
    settings.entrySet().asScala.map(x => (x.getKey, x.getValue)).toArray
  }

  /** Get a parameter as an integer, falling back to a default if not set */
  def getInt(key: String, defaultValue: Int): Int = {
    getOption(key).map(_.toInt).getOrElse(defaultValue)
  }

  /** Get a parameter as a long, falling back to a default if not set */
  def getLong(key: String, defaultValue: Long): Long = {
    getOption(key).map(_.toLong).getOrElse(defaultValue)
  }

  /** Get a parameter as a double, falling back to a default if not set */
  def getDouble(key: String, defaultValue: Double): Double = {
    getOption(key).map(_.toDouble).getOrElse(defaultValue)
  }

  /** Get a parameter as a boolean, falling back to a default if not set */
  def getBoolean(key: String, defaultValue: Boolean): Boolean = {
    getOption(key).map(_.toBoolean).getOrElse(defaultValue)
  }

  /** Does the configuration contain a given parameter? */
  def contains(key: String): Boolean = settings.containsKey(key)

  /** Copy this object */
  override def clone: AppConf = {
    new AppConf(false).setAll(getAll)
  }

  /**
   * By using this instead of System.getenv(), environment variables can be mocked
   * in unit tests.
   */
  private def getenv(name: String): String = System.getenv(name)

  /**
   * Return a string listing all keys and values, one per line. This is useful to print the
   * configuration out for debugging.
   */
  def toDebugString: String = {
    getAll.sorted.map{case (k, v) => k + "=" + v}.mkString("\n")
  }

}
