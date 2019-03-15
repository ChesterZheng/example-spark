package ipLocation

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 按“省份”统计每个省份的客户量
  */
object IpLocation {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("IpLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)

    //加载IP规则
    val ipRulesRdd = sc.textFile("E://BigData-test/spark-test/ip.txt").map (line => {
      val fields = line.split("\\|")
      val start_num = fields(2)
      val end_num = fields(3)
      val province = fields(6)
      (start_num, end_num, province)
    })

    val ipRulesArray = ipRulesRdd.collect()

    //广播规则
    val ipRulesBroadCast = sc.broadcast(ipRulesArray)

    //加载要处理的数据
    val ipsRDD = sc.textFile("E://BigData-test/spark-test/access_log").map(line => {
      val fields = line.split("\\|")
      fields(1)
    })

    val result = ipsRDD.map(ip => {
      val ipNum = ip2Long(ip)
      val index = binarySearch(ipRulesBroadCast.value, ipNum)
      val info = ipRulesBroadCast.value(index)
      info
    }).map(t => (t._3,1)).reduceByKey(_+_)

    //向MySql写入数据
    result.foreachPartition(data2MySQL(_))
  }

  /**
    * IP地址转换为Long类型
    *
    */
  def ip2Long(ip: String): Long = {

    val fragments = ip.split("[. ]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 二分法查找数据
    *
    * @return
    */
  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else
        low = middle + 1
    }
    -1
  }

  /**
    * 将结果写入MySQL
    */
  val data2MySQL = (iterator: Iterator[(String, Int)]) => {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    val sql ="INSERT INTO location_info (location, counts, access_date) VALUES (?,?,?)"

    try {
      connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata", "root", "root")
      iterator.foreach(line => {
        preparedStatement = connection.prepareStatement(sql)
        preparedStatement.setString(1, line._1)
        preparedStatement.setInt(2, line._2)
        preparedStatement.setDate(3, new Date(System.currentTimeMillis()))
        preparedStatement.executeUpdate()
      })
    } catch {
      case e: Exception => println("MySQL Exception")
    }finally {
      if(preparedStatement != null)
        preparedStatement.close()
      if(connection != null)
        connection.close()
    }
  }

}
