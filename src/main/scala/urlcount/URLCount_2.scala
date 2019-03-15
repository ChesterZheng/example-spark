package urlcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 小示例
  * 第二种方式
  * 求URL的Top N
  * 在数据量大的时候，此种方式性能会好一些，且避免内存溢出
  */
object URLCount_2 {

  def main(args: Array[String]) {

    val urls = Array("http://java.itcast.cn","http://php.itcast.cn","http://net.itcast.cn")

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

    //循环过滤
    for (u <- urls) {
      val insRDD = summedURL.filter(tuple => {
        val url = tuple._1
        url.startsWith(u)
      })
      val result = insRDD.sortBy(_._2, false).take(3)
      println(result.toBuffer)
    }

    sc.stop()

  }
}
