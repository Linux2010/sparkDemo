package cn.com.yusys.itemCF

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//基于物品推荐
object ItemCF {
  def main(args: Array[String]): Unit = {
    //1.构建Spark对象
    val conf: SparkConf = new SparkConf().setAppName("ItemCF").setMaster("local[*]")
    val sc = new SparkContext(conf)
    Logger.getRootLogger.setLevel(Level.WARN)

    //2.读取数据
    val data_path = "/Users/mac/Documents/workspace/sparkDemo/src/main/scala/cn/com/yusys/itemCF/sample_itemCF.txt"
    val data: RDD[String] = sc.textFile(data_path)
    val user_data: RDD[ItemPref] = data.map(_.split(",")).map(f => (ItemPref(f(0), f(1), f(2).toDouble))).cache()

    //3.建立模型
    val mysimil = new ItemSimilarity()
    val simil_rdd1: RDD[ItemSimi] = mysimil.Similarity(user_data,"cooccurrence")
    val recommd = new RecommendedItem
    val recommd_rdd1: RDD[UserRecomm] = recommd.Recommend(simil_rdd1,user_data,30)

    //4.打印结果
    println(s"物品相似度矩阵(物品i,物品j,相似度)：${simil_rdd1.count()}")
    simil_rdd1.collect().foreach{ ItemSimi =>
      println(ItemSimi.itemid1+","+ItemSimi.itemid2+","+ItemSimi.similar)
    }
    println(s"用户推荐列表(用户,物品,推荐值)：${recommd_rdd1.count()}")
    recommd_rdd1.collect().foreach{
      UserRecomm=> println(UserRecomm.userid+","+UserRecomm.itemid+","+UserRecomm.pref)
    }

    sc.stop()
  }
}

