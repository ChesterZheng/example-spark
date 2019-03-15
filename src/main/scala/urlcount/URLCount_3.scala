package urlcount

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * 小示例
  * 第三种方式
  * 求URL的Top N
  */
object URLCount_3 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("URLCount_1").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile(args(0))
    //切分数据
    val urlWithOne = lines.map(line => {
      val fields = line.split("\t")
      val url = fields(1)
      (url, 1)
    })
    //聚合
    val summedURL: RDD[(String, Int)] = urlWithOne.reduceByKey(_ + _)

    println(summedURL.collect().toBuffer)

    val URLRDD = summedURL.map(tuple => {
      val host = new URL(tuple._1).getHost
      (host, (tuple._1, tuple._2))
    })

    val urls: Array[String] = URLRDD.map(_._1).distinct().collect()

    val partitioner = new HostPartitioner(urls)

    //按照自定义Partitioner分区器重新分区
    val repartitionedRDD: RDD[(String, (String, Int))] = URLRDD.partitionBy(partitioner)

    val result = repartitionedRDD.mapPartitions(iterator => {
      iterator.toList.sortBy(_._2._2).reverse.take(3).iterator
    })

    println(result)

    result.saveAsTextFile("D://URL-Count-Out")

    sc.stop()

  }
}

/**
  * 自定义Partitioner
  * 【注意】：Driver端不存在多线程问题！！！
  */
class HostPartitioner(urls: Array[String]) extends Partitioner {

  val rulesMap = new mutable.HashMap[String, Int]()

  var index = 0

  for (url <- urls) {
    rulesMap.put(url, index)
    index += 1
  }

  override def numPartitions: Int = urls.length

  override def getPartition(key: Any): Int = {

    val url = key.toString
    rulesMap.getOrElse(url, 0)
  }
}
