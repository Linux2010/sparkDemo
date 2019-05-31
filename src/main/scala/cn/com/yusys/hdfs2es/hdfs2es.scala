package cn.com.yusys.hdfs2es

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.elasticsearch.spark.sql.EsSparkSQL

object hdfs2es {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("rdd2df").setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "172.16.60.41:9200,172.16.60.42:9200,172.16.60.43:9200")
    //sparkConf.set("es.port", "9200")
    sparkConf.set("es.index.read.missing.as.empty", "true")
    sparkConf.set("es.nodes.wan.only", "true")
    sparkConf.set("es.net.http.auth.user", "") //访问es的用户名
    sparkConf.set("es.net.http.auth.pass", "") //访问es的密码
    new SparkContext(sparkConf)
    val spark = SparkSession.builder().getOrCreate().sqlContext

    val peopleRDD = spark.sparkContext.textFile("hdfs://yuxin/1.txt")
    val schemaString = "id name age "

    // Generate the schema based on the string of schema
    val fields = schemaString.split(" ")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)


    val rowRDD = peopleRDD
      .map(_.split(","))
      .map(attributes => {
        var row :Row = Row()
          for(i <- 0 until attributes.length){
            row = Row.merge(row,Row(attributes(i)))
          }
        row
      })

    val peopleDF = spark.createDataFrame(rowRDD, schema)

    peopleDF.printSchema()
    peopleDF.show
    EsSparkSQL.saveToEs(peopleDF, "hdfs2e")

  }
}
