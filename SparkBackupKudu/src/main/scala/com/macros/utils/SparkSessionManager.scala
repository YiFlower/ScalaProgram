package com.macros.utils

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

/**
 * Data: 2021-08-30
 * Function: get Session and close Session
 * Auther: YiHua
 * Version: 1.0
 * */
object SparkSessionManager extends LoggerLevel {
  System.setProperty("hadoop.home.dir", "E:\\Project\\IDEA\\spark-2.4.4-bin-hadoop2.6")
  private val log: Logger = Logger.getLogger(SparkSessionManager.getClass)

  def getLocalSparkSession(appName: String): SparkSession = {
    SparkSession.builder().appName(appName).getOrCreate()
  }

  // 开启Hive支持
  def getLocalSparkSession(appName: String, support: Boolean): SparkSession = {
    if (support) SparkSession.builder().appName(appName).enableHiveSupport().getOrCreate()
    else getLocalSparkSession(appName)
  }

  // 关闭SparkSession
  def stopSpark(ss: SparkSession) = {
    if (ss != null) {
      try {
        ss.stop()
        log.info("SparkSesion关闭成功！")
      } catch {
        case e: Exception => {
          log.error("SparkSesion关闭失败！" + e.getMessage)
        }
      }
    }
  }
}