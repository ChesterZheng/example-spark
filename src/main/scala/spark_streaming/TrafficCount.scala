package spark_streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

case class DataBean(session_count: Int,
                    retrans_count: Long,
                    connect_time: Long,
                    resp_delay: Long,
                    total_packets: Long,
                    retrans_packets: Int,
                    up_bytes: Int,
                    down_bytes: Int) extends Serializable

/**
  * Spark Streaming 窗口函数
  */
object TrafficCount {


  def reduceFunc = (a: DataBean, b: DataBean) => {
    DataBean(a.session_count + b.session_count,
      a.retrans_count + b.retrans_count,
      a.connect_time + b.connect_time,
      a.resp_delay + b.resp_delay,
      a.total_packets + b.total_packets,
      a.retrans_packets + b.retrans_packets,
      a.up_bytes + b.up_bytes,
      a.down_bytes + b.down_bytes)
  }

  def main(args: Array[String]) {

    LoggerLevels.setStreamingLogLevels()

    val Array(zkQuorum, group, topics, numThreads) = args

    val conf = new SparkConf().setAppName("TrafficCount").setMaster("local[2]")

    val sc = new StreamingContext(conf, Seconds(30))

    sc.checkpoint("E://spark-test/KafkaStreamingWordCount")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap

    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(sc,
      zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER)

    val lines: DStream[String] = data.map(_._2)

    val tupleData: DStream[((String, String), DataBean)] = lines.map(line => {

      val fields = line.split(",")

      //需要的字段
      ((fields(0), fields(1)), DataBean(fields(5).toInt, fields(6).toLong,
        fields(7).toLong, fields(10).toLong, fields(11).toLong, fields(12).toInt,
        fields(16).toInt, fields(17).toInt))
    })

    val result: DStream[((String, String), DataBean)] = tupleData.reduceByKeyAndWindow(reduceFunc, Seconds(180), Seconds(150))
    result.foreachRDD(rdd => {
      rdd.foreachPartition(part => {

        //将下面的代码修改一下表名就可以了
        //val hConf = new HBaseConfiguration()
        //val hTable = new HTable(hConf, "table")
        part.foreach(x => {
          //修改一下对应的列名就可以了
          //val thePut = new Put(Bytes.toBytes(row(0)))
          //thePut.add(Bytes.toBytes("cf"), Bytes.toBytes(row(0)), Bytes.toBytes(row(0)))
          //hTable.put(thePut)
        })
        //htable.close()
      })
    })
    sc.start()
    sc.awaitTermination()

  }
}
