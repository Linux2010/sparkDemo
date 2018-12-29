package cn.com.yusys

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * @项目名称: spark01
  * @类名称: cn.com.yusys
  * scala静态方法
  */
object SparkTest {

  def getSqlContext(): SQLContext = {
    //1)创建context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    return sqlContext
  }

  def printToSceen(path: String): Unit = {
    //1)创建context
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SQLContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    //2)操作
    val textFileRdd = sc.textFile(path)
    textFileRdd.map(line => line.toString)
    textFileRdd.foreach(println(_))
  }

}
