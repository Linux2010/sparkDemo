package cn.com.yusys.autoPrice

import org.apache.spark.sql.SparkSession

object AutoPriceDemo extends App {
  val spark = SparkSession.builder().appName("AutoPriceDemo").master("local").getOrCreate()
  val sqlContext = spark.sqlContext
  val str = "美国末日 火箭联盟 命运2 杀戮"
  val rdd = sqlContext.sparkContext.textFile("listtodatabase1.csv")
  val arry= str.split(" ")

}
