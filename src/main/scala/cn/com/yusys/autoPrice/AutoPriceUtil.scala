package cn.com.yusys.autoPrice

import org.apache.spark.sql.SparkSession
import org.apache.commons.lang.StringUtils


object AutoPriceUtil {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("AutoPriceAPP").master("local").getOrCreate()
    val sqlContext = spark.sqlContext
    val rdd = sqlContext.sparkContext.textFile("src/main/resources/list.txt")
    val rdd2 = rdd.map(row => {
      //println(row)
      val row3 = StringUtils.substringBefore(row, "/")
      println(row3)
      row3
      //val row4 = row3.filterNot(_.isEmpty)
      // println(row4)
    })
    rdd2.saveAsTextFile("src/main/resources/list2.txt")
    //    val rdd2 = rdd.map(row=>{
    //       row.split(" ").filterNot(_.isEmpty)
    //    })
    //    rdd2.foreach(arr=>{
    //      for(i<-0 until arr.length) print(arr(i)+",")
    //      println(" ")
    //    })
    //使用正则表达式提取数字
  }
}
