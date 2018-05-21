package common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.internal.Logging

/**
  * @author YKL on 2018/3/23.
  * @version 1.0
  *          说明：
  *          XXX
  */
object LoggerLevels extends Logging {
  //直接调用时默认为WARN，可配置其他级别，如ERROR
  def setLogLevels(level: Level = Level.WARN): Unit = {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo(s"Setting log level to [$level] for this application " +
        "override add a custom log4j.properties to the classpath")
      Logger.getRootLogger.setLevel(level)
    }
  }

}
