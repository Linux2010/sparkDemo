package cn.com.yusys

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext

/**
  * @项目名称: spark01
  * @类名称: cn.com.yusys
  * @类描述:
  * @功能描述:
  * @创建人: tianfs1@yusys.com.cn
  * @创建时间: 2018/12/26
  * @修改备注:
  * @修改记录: 修改时间    修改人员    修改原因
  *        -------------------------------------------------------------
  * @version 1.0.0
  * @Copyright (c) 2018宇信科技-版权所有
  */
object Sql {
  def main(args: Array[String]): Unit = {

    val path =args(0)
    println(path)
    //1)创建context
    val sparkConf = new SparkConf()
    //sparkConf.setAppName("SQLContextApp").setMaster("local[2]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    val hiveContext = new HiveContext(sc)
    //2)操作
    val people = sqlContext.read.format("json").load(path)
    people.printSchema()
    people.show()
    //3)关闭资源
    sc.stop()
  }
}
