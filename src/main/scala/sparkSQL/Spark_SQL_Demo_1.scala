package sparkSQL

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Spark-Sql示例
  * 参数：args(0)为要处理的数据目录
  *      args(1)为结果输出目录
  *
  */
object Spark_SQL_Demo_1 {

  def main(args: Array[String]) {

    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("SQL-1").setMaster("local[2]")

    //创建SparkContext，SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)

    //创建SQLContext
    val sqlContext = new SQLContext(sc)

    //从指定的地址创建RDD
    val lineRDD = sc.textFile(args(0)).map(_.split(" "))

    //创建case class

    //将RDD和case class关联
    val personRDD = lineRDD.map(arr => Person(arr(0), arr(1), arr(2)))

    //导入隐式转换，否则无法将RDD转换成DataFrame
    import sqlContext.implicits._

    //将RDD转换成DataFrame
    val personDataFrame = personRDD.toDF

    //注册临时表
    personDataFrame.registerTempTable("t_person")

    //传入SQL
    val dataFrame = sqlContext.sql("select * from t_person order by age desc limit 2")

    //将结果以JSON的方式存储到指定的位置
    dataFrame.write.json(args(1))

    //停止SparkContext
    sc.stop()

  }

}

case class Person(id: String, name: String, age: String)


