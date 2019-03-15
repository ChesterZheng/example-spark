package com.test.spark.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Chester on 2016/12/21.
  */
object WordCount {

  def main(args: Array[String]) {

    //创建SparkConf
    val config = new SparkConf().setAppName("WordCount")
    //创建SparkContext
    val sc = new SparkContext(config)
    //读取HDFS中的数据
    val lines: RDD[String] = sc.textFile(args(0))
    //切分单词
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词计数
    val wordAndCountMap: RDD[(String, Int)] = words.map((_, 1))
    //分组聚合
    val result: RDD[(String, Int)] = wordAndCountMap.reduceByKey(_ + _)
    //排序
    val finalResult: RDD[(String, Int)] = result.sortBy(_._2)
    //将数据存储到HDFS中
    finalResult.saveAsTextFile(args(1))
    //释放资源
    sc.stop()

  }

}
