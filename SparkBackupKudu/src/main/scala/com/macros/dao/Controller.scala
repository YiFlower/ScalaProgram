package com.macros.dao

import com.macros.utils.{LoggerLevel, SparkSessionManager}
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Controller extends LoggerLevel {
  private val log: Logger = Logger.getLogger(Controller.getClass)
  private val ss: SparkSession = SparkSessionManager.getLocalSparkSession("Controller", true)
  // 做动态分区, 所以要先设定partition参数
  // default是false, 需要额外下指令打开这个开关
  ss.sqlContext.setConf("hive.exec.dynamic.partition;", "true");
  // 非严格模式
  ss.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict");
  // 设置关闭分桶与排序操作，否则写入hive会报错：
  // ... is bucketed but Spark currently does
  // NOT populate bucketed output which is compatible with Hive
  ss.sqlContext.setConf("hive.enforce.bucketing","false")
  ss.sqlContext.setConf("hive.enforce.sorting","false")
  log.info("获取SparkSession:" + ss)

  def kuduToHive(kuduTableName: String, hiveTableName: String, backUpData: String): Unit = {
    val kuduConfig = ss.read.format("org.apache.kudu.spark.kudu")
      .option("kudu.master", "10.110.32.1:7051,10.110.32.2:7051,10.110.32.3:7051")
      //.option("kudu.master", "10.168.1.38:7051")
      .option("kudu.table", "impala::" + kuduTableName) // Tips：注意指定库
      .load()
    // create view
    log.info("创建createTempView：kuduTmpView")
    kuduConfig.createTempView("kuduTmpView")

    // query
    val frame = ss.sql(
      s"""
         |SELECT * FROM `kuduTmpView`
         |WHERE splittime = "${backUpData}"
         |""".stripMargin)

    frame.createOrReplaceTempView("kuduBackupView")
    log.info("创建createOrReplaceTempView：kuduBackupView")

    // insert hive
    log.info("=== 执行插入SQL ===")
    // 如果不加use db，会有找不到表或视图不存在的问题
    ss.sql(
      """
        |USE `realtimebakup`
        |""".stripMargin)

    ss.sql(
      s"""
         |INSERT INTO ${hiveTableName} PARTITION(splittime)
         |  SELECT
         |  id,packtimestr,dcs_name,dcs_type,dcs_value,dcs_as,dcs_as2,splittime
         |  FROM `kuduBackupView`
         |""".stripMargin)

    // close ss
    SparkSessionManager.stopSpark(ss)
  }
}