package spark_streaming

import java.net.InetSocketAddress

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.flume.{FlumeUtils, SparkFlumeEvent}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming ingests Flume
  * WordCount
  * 拉取方式
  */
object FlumeStreamingWordCount {

  def main(args: Array[String]) {

    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("FlumeStreamingWordCount").setMaster("local[2]")

    //创建StreamingContext并设置产生批次的间隔时间
    val sc = new StreamingContext(conf, Seconds(10))

    val flumeStream: ReceiverInputDStream[SparkFlumeEvent] = FlumeUtils.createPollingStream(sc,
      Array(new InetSocketAddress("CentOS-Mini01", 8888)), StorageLevel.MEMORY_AND_DISK)

    //获取Flume中的数据
    val words: DStream[String] = flumeStream.flatMap(
      x => new String(x.event.getBody.array()).split(" "))

    val wordAndCount: DStream[(String, Int)] = words.map((_, 1))

    val result: DStream[(String, Int)] = wordAndCount.reduceByKey(_ + _)

    result.print()

    //启动程序
    sc.start()

    //等待结束
    sc.awaitTermination()
  }

}
