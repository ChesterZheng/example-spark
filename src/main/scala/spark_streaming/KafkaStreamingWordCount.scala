package spark_streaming

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Spark Streaming ingests Kafka
  * WordCount
  */
object KafkaStreamingWordCount {

  val updateFunc = (iterator: Iterator[(String, Seq[Int], Option[Int])]) => {
    iterator.flatMap {
      case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(i => (x, i))
    }
  }


  def main(args: Array[String]) {

    LoggerLevels.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args

    val conf = new SparkConf().setAppName("KafkaStreamingWordCount").setMaster("local[2]")

    val sc = new StreamingContext(conf, Seconds(10))

    sc.checkpoint("E://BigData-test/spark-test/KafkaStreamingWordCount")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(sc,
      zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val words = data.map(_._2).flatMap(_.split(" "))

    val result: DStream[(String, Int)] = words.map((_, 1)).updateStateByKey(updateFunc,
      new HashPartitioner(sc.sparkContext.defaultParallelism), rememberPartitioner = true)

    result.print()

    sc.start()

    sc.awaitTermination()

  }

}
