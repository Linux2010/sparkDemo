package cn.com.yusys

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/9/26
  */
object DataFrameTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("DataFrameTest").master("local[2]").getOrCreate()
    val sc =  spark.sparkContext

    val rdd1 = sc.parallelize(Array(Row(1,1,null),Row(1,2,"zhang12"), Row(2,2,"lishi22"), Row(3,3,"wangwu33"),Row(3,1,"wangwu31"),Row(4,4,"zhaoliu44")))

    val rdd2 = sc.parallelize(Array(Row(1,1,"zhang11"),Row(1,2,"zhang12修改"),Row(1,3,"zhang13新增"), Row(2,2,"lishi22"), Row(3,3,"wangwu33"),Row(3,1,"wangwu31"),Row(5,4,"zhaoliu54新增")))


    val schema = StructType(Array(StructField("id", DataTypes.IntegerType), StructField("age", DataTypes.IntegerType),StructField("name", DataTypes.StringType)))

    val ds1 = spark.createDataFrame(rdd1, schema)
    val ds2 = spark.createDataFrame(rdd2, schema)
    ds1.show()
    ds2.show()

    val newds = ds2.except(ds1);//修改后，新增
    val oldds = ds1.except(ds2);//修改前，删除
    newds.show()
    oldds.show()

    newds.createOrReplaceTempView("new_table")
    oldds.createOrReplaceTempView("old_table")

    val newdsUpdate = spark.sql("select nt.id,nvl(nt.age,''),nvl(nt.name,'') from new_table nt join old_table ot on nt.id=ot.id and nt.age=ot.age")
    newdsUpdate.show()//修改
    val olddsUpdate = spark.sql("select nvl(id,'') as id,nvl(age,'') as age,nvl(nt.name,'') as name from old_table nt join old_table ot on nt.id=ot.id and nt.age=ot.age")
    olddsUpdate.show()
    val newDsInsert = newds.except(newdsUpdate);//新增
    newDsInsert.show()
    val oldDsDel = oldds.except(olddsUpdate); //删除
    oldDsDel.show()
  }


}
