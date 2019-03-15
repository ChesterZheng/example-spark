package customSort

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 通过“隐式值”的方式排序
  */
object OrderContext {

  implicit val girlOrdering = new Ordering[Girl] {
    override def compare(x: Girl, y: Girl): Int = {
      if (x.faceValue > y.faceValue) {
        1
      } else if (x.faceValue == y.faceValue) {
        if (x.age > y.age) {
          -1
        } else {
          1
        }
      } else {
        -1
      }
    }
  }
}

object CustomSort {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("CustomSort").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("tom", 90, 28, 1), ("jack", 90, 27, 2), ("mary", 95, 22, 3)))
    import OrderContext._
    val rdd2: RDD[(String, Int, Int, Int)] = rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(rdd2.collect().toBuffer)
    sc.stop()
  }
}

///**
//  *
//  * @param faceValue
//  * @param age
//  */
//case class Girl(faceValue: Int, age: Int) extends Ordered[Girl] with Serializable {
//  override def compare(that: Girl): Int = {
//       if(this.faceValue == that.faceValue) {
//         that.age - this.age
//       } else {
//         this.faceValue - that.faceValue
//       }
//  }
//}


/**
  * 通过“隐式值”的方式进行排序
  *
  */
case class Girl(faceValue: Int, age: Int) extends Serializable

