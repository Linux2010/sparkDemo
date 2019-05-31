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


//  private def runProgrammaticSchemaExample(spark:SparkSession): Unit ={
//    // 1.转成RDD
//    val rdd = spark.sparkContext.textFile("E:/大数据/data/people.txt")
//
//    // 2.定义schema，带有StructType的
//    // 定义schema信息
//    val schemaString = "name age"
//    // 对schema信息按空格进行分割
//    // 最终fileds里包含了2个StructField
//    val fields = schemaString.split(" ")
//      // 字段类型，字段名称判断是不是为空
//      .map(fieldName => StructField(fieldName, StringType, nullable = true))
//    val schema = StructType(fields)
//
//    // 3.把我们的schema信息作用到RDD上
//    //   这个RDD里面包含了一些行
//    // 形成Row类型的RDD
//    val rowRDD = rdd.map(_.split(","))
//      .map(x => Row(x(0), x(1).trim))
//    // 通过SparkSession创建一个DataFrame
//    // 传进来一个rowRDD和schema，将schema作用到rowRDD上
//    val peopleDF = spark.createDataFrame(rowRDD, schema)
//
//    peopleDF.show()
//  }

}
