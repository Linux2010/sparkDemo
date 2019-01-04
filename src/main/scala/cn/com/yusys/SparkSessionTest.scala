package cn.com.yusys

import org.apache.spark.sql.SparkSession

/**
  * @项目名称: spark01
  * @类名称: cn.com.yusys
  * @类描述:
  * @功能描述:
  * @创建人: tianfs1@yusys.com.cn
  * @创建时间: 2018/12/29
  * @修改备注:
  * @修改记录: 修改时间    修改人员    修改原因
  *        -------------------------------------------------------------
  * @version 1.0.0
  * @Copyright (c) 2018宇信科技-版权所有
  */
object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate()
    val people = spark.read.json("C:\\Users\\94946\\Desktop\\people.json")
    people.show()
    spark.stop()
  }
}
