package cn.com.yusys

import org.apache.spark.sql.SparkSession

/**
  * @date 2019/1/4
  * @author 94946
  * @DESC dataFrame 和 RDD 的相互操作
  */
object DataFarmeRDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local").getOrCreate()
    //rdd --> dateFarme
    val rdd = spark.sparkContext.textFile("D:\\IdeaWorkSpaces\\spark01\\src\\main\\resources\\1.txt")
    //导入隐式转换
    import  spark.implicits._
    val infoDF =  rdd.map(_.split(",")).map(line => Info(line(0).toInt,line(1),line(2).toInt)).toDF()
    infoDF.show()
    spark.stop()
  }
  case class Info(id:Int,name:String,age: Int)

}
