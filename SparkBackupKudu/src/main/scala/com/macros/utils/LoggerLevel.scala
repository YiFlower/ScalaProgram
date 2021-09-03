package com.macros.utils

import org.apache.log4j.{Level, Logger}

trait LoggerLevel {
  Logger.getLogger("org").setLevel(Level.WARN)
}
