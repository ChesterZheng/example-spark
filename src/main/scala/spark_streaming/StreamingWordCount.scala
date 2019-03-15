package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 简单的WordCount
  */
object StreamingWordCount {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[2]")

    //创建StreamingContext并设置产生批次的间隔时间
    val sc = new StreamingContext(conf, Seconds(5))

    //从Socket端口中创建RDD
    val lines: ReceiverInputDStream[String] = sc.socketTextStream("192.168.1.50",11111)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndCount: DStream[(String, Int)] = words.map((_, 1))

    val result: DStream[(String, Int)] = wordAndCount.reduceByKey(_+_)

    result.print()

    //启动程序
    sc.start()

    //等待结束
    sc.awaitTermination()
  }

}
