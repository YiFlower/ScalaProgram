package com.macros.main

import com.macros.dao.Controller
import com.macros.utils.LoggerLevel
import org.apache.log4j.Logger

object JustMain extends LoggerLevel {
  private val log: Logger = Logger.getLogger(JustMain.getClass)

  def main(args: Array[String]): Unit = {
    if (args.length != 3 || args == null) {
      log.error("请正确输入参数！arg1: kudu table name,arg2: hive table name,arg3: backup date")
      System.exit(1)
    }
    try {
      log.info("Input Params : KuduTbName,HiveTbName,BackUpDate <===> " + args(0), args(1), args(2))
      Controller.kuduToHive(args(0), args(1), args(2))
    } catch {
      case e: Exception =>
        log.error(e.getMessage)
        e.printStackTrace()
        System.exit(1)
    }
  }
}