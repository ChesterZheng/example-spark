package spark_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
  * 设置日志打印级别
  */
object LoggerLevels extends Logging{

  def setStreamingLogLevels(): Unit = {

    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements

    if(!log4jInitialized){

      logInfo("Setting log level to [WARN] for streaming example." +
        "To override add a custom log4j.properties to the class path.")

      Logger.getRootLogger.setLevel(Level.WARN)
    }

  }
}
