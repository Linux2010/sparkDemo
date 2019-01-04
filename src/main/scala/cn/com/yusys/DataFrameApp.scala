package cn.com.yusys

import org.apache.spark.sql.SparkSession

/**
  * @date 2019/1/4
  * @author 94946
  * @DESC dataFrame api 基本操作
  */
object DataFrameApp {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameApp").master("local").getOrCreate()
    //将json文件转成dataframe
    val peopleDF = spark.read.format("json").load("C:\\Users\\94946\\Desktop\\people.json")
    peopleDF.printSchema()

    peopleDF.show()

    peopleDF.select("name").show()

    //select age+10 as age2 from table
    peopleDF.select((peopleDF.col("age")+10).as("age2")).show()

    //select * from table where age>19
    peopleDF.filter(peopleDF.col("age")>19).show()



    spark.stop()

  }
}
