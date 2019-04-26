package cn.com.yusys

import org.apache.spark.sql.SparkSession

import scala.xml.Properties

/**
  * @date 2019/1/4
  * @author 94946
  * @DESC dataFrame 和 RDD 的相互操作
  */
object DataFarmeRDDApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameApp").master("local").getOrCreate()
    //rdd --> dateFarme
    val rdd = spark.sparkContext.textFile("src/main/resources/1.txt")
    //导入隐式转换
    import  spark.implicits._
    val infoDF =  rdd.map(_.split(",")).map(line => Info(line(0).toInt,line(1),line(2).toInt)).toDF()
    val infoDF2 =  rdd.map(_.split(",")).map(line => Info(line(0).toInt,line(1),line(2).toInt)).toDF()
    infoDF.show()
    infoDF.filter("name='nihao'").show(10)
    infoDF.sort(infoDF("id").desc).show()

    //infoDF.join(infoDF2,infoDF.col("id")===infoDF2.col("id"),"inner").write.mode("Overwrite").jdbc("jdbc:mysql://172.16.60.41:3306/source", "PERSON_INFO", connectionProperties)
    spark.stop()
  }
  case class Info(id:Int,name:String,age: Int)
}
