package cn.com.yusys.spark2es

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.EsSparkSQL

object Mysql_To_ES {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Mysql_To_ES").setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "172.16.60.42")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.index.read.missing.as.empty", "true")
    sparkConf.set("es.nodes.wan.only", "true")
    val sc = new SparkContext(sparkConf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val prop = new Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

   // val arr = Array("1=1 limit 1,2", "1=1 limit 3,2")

    val dataFrame = sqlContext.read.jdbc("jdbc:mysql://172.16.60.41:3306/source", "(select * from person_info)aa", prop)

    dataFrame.printSchema()
    //EsSpark.saveToEs(dataFrame.rdd, "mysqlTest/_doc");

   // EsSparkSQL.saveToEs(dataFrame, "mysql_test/person_info")

  }

}
