package cn.com.yusys

import cn.com.yusys.DataFarmeRDDApp.Info
import org.apache.spark.sql.SparkSession

/**
  * @date 2019/1/15
  * @author 94946
  * @DESC dateframe 中的其他操作
  */
object DataFrameCase {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("DataFrameCase").master("local").getOrCreate()
    //rdd --> dateFarme
    val rdd = spark.sparkContext.textFile("src/main/resources/1.txt")
    //导入隐式转换
    import spark.implicits._
    val studentDF = rdd.map(_.split("\\|")).map(line => Sutdent(line(0).toInt, line(1), line(2),line(3))).toDF()
    //show 默认只显示前20条 0
    studentDF.show()
    spark.stop()
  }
case class Sutdent(id:Int,name:String,phone:String ,email:String)
}
