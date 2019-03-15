package spark_streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 可更新状态的WordCount
  */
object StreamingWordCountWithStatement {

  val updateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {

    //    iterator.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    iterator.map {
      case (x, y, z) => (x, y.sum + z.getOrElse(0))
    }
  }

  def main(args: Array[String]) {

    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("StreamingWordCountWithStatement").setMaster("local[2]")

    val sc = new StreamingContext(conf, Seconds(5))

    //【注意】：设置checkpoint目录(原因：防止程序运行时出现问题导致之前的结果数据丢失)
    sc.checkpoint("E://BigData-test/spark-test/StreamingWordCountWithStatement")

    val lines: ReceiverInputDStream[String] = sc.socketTextStream("192.168.1.50", 8888)

    val words: DStream[String] = lines.flatMap(_.split(" "))

    val wordAndCount: DStream[(String, Int)] = words.map((_, 1))

    val result: DStream[(String, Int)] = wordAndCount.updateStateByKey(
      updateFunc,
      new HashPartitioner(sc.sparkContext.defaultParallelism),
      rememberPartitioner = true
    )

    result.print()

    sc.start()

    sc.awaitTermination()

  }

}
