package sparkSQL

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过StructType映射成为表
  */
object Spark_SQL_Demo_2 {

  def main(args: Array[String]) {

    //创建SparkConf()并设置App名称
    val conf = new SparkConf().setAppName("SQL-1").setMaster("local[2]")

    //创建SparkContext，SQLContext要依赖SparkContext
    val sc = new SparkContext(conf)

    //创建SQLContext
    val sqlContext = new SQLContext(sc)

    //从指定的地址创建RDD
    val lineRDD = sc.textFile(args(0)).map(_.split(" "))

    //通过StructType直接指定每个字段的schema
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("age", IntegerType, true)
      )

    )

    val personRDD = lineRDD.map(arr => Row(arr(0).toInt, arr(1).trim, arr(2).toInt))

    //将schema信息应用到personRDD上
    val personDataFrame = sqlContext.createDataFrame(personRDD, schema)

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
