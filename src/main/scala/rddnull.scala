import java.util

import org.apache.spark.sql.SparkSession
import org.apache.spark._



/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/12/6
  */
object rddnull {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("rddTest").master("local[*]").getOrCreate()
    val sc =  spark.sparkContext

    val rdd = sc.emptyRDD[String]
    val rdd1 = sc.parallelize(List("123","1234"))
    val rdd2 = rdd.union(rdd1)
    rdd2.foreach(s=>println(s))
    //var list = new util.ArrayList[String]()

    //list.add("ssd")
    //import scala.collection.JavaConverters._
    //val rdd =sc.parallelize(list.asScala)
    //rdd.foreach(s=>println(s))

  }

}
