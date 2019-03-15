package sparkSQL

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 使用原生的RDD读取关系型数据库中的数据
  * 使用JdbcRDD请看JdbcRDD的构造参数
  */
object Spark_SQL_Demo_3 {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("JdbcRDD").setMaster("local[2]")

    val sc = new SparkContext(conf)

    val connection = () => {

      Class.forName("com.mysql.jdbc.Driver").newInstance()

      DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "root")
    }

    val jdbcRDD = new JdbcRDD(
      sc,
      connection,
      "SELECT * FROM person p WHERE p.id >= ? And p.id <= ? ORDER BY p.age desc",
      1,4,2,
      r => {
        val id = r.getInt(1)
        val name = r.getString(2)
        val age = r.getInt(3)
        (id, name, age)
      })

    val resultRDD = jdbcRDD.collect()

    println(resultRDD.toBuffer)

    sc.stop()
  }

}
