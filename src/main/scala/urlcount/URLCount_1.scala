package urlcount

import java.net.URL

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 小示例
  * 第一种方式
  * 求URL的Top N
  */
object URLCount_1 {

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
    val summedURL: RDD[(String, Int)] = urlWithOne.reduceByKey(_+_).cache()

    println(summedURL.collect().toBuffer)

    val groupedURL = summedURL.map(tuple => {
      val host = new URL(tuple._1).getHost
      (host,tuple._1, tuple._2)
    }).groupBy(_._1)

    //toList是将数据全部加载到内存中，当数据量很大的时候，可能会内存溢出
    val result = groupedURL.mapValues(_.toList.sortBy(_._3).reverse.take(3))

    println(result.collect().toBuffer)

    sc.stop()

  }
}
