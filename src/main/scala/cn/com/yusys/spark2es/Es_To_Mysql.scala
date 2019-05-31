package cn.com.yusys.spark2es

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.sql.EsSparkSQL

object Es_To_Mysql {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("Mysql_To_ES").setMaster("local[2]")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes", "192.168.56.101")
    sparkConf.set("es.port", "9200")
    sparkConf.set("es.index.read.missing.as.empty", "true")
    sparkConf.set("es.nodes.wan.only", "true")
    val sc = new SparkContext(sparkConf)
    val sqlContext = SparkSession.builder().getOrCreate().sqlContext

    val prop = new Properties()
    prop.setProperty("user", "mytest")
    prop.setProperty("password", "mytest")
    prop.setProperty("driver", "com.mysql.jdbc.Driver")

    val dataFrame = EsSparkSQL.esDF(sqlContext, "mysql_test/_doc")

    dataFrame.createOrReplaceTempView("mysql_test")
    dataFrame.printSchema()
    dataFrame.foreach(println(_))

    val frame = sqlContext.sql("select * from mysql_test")
    frame.show()

    //frame.write.jdbc("jdbc:mysql://127.0.0.1/mytest","elasticsearch",prop)

    //frame.rdd.saveAsTextFile("hdfs://engine:9000/elasticsearch")
  }

}
