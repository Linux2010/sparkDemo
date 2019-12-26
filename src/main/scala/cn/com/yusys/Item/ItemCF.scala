package cn.com.yusys.Item

import _root_.breeze.numerics.sqrt
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

/**
  * 用户评分
  * @param userid 用户
  * @param itemid 物品
  * @param pref 评分
  */
case class ItemPref(val userid : String,val itemid : String, val pref : Double) extends Serializable

/**
  * 相似度
  * @param itemid_1 物品
  * @param itemid_2 物品
  * @param similar 相似度
  */
case class ItemSimilar(val itemid_1 : String, val itemid_2 : String, val similar : Double) extends Serializable

/**
  * 给用户推荐物品
  * @param userid 用户
  * @param itemid 物品
  * @param pref 推荐系数
  */
case class UserRecommend(val userid : String, val itemid : String, val pref : Double) extends Serializable

/**
  * 相似度计算
  */
class ItemSimilarity extends Serializable{
  def Similarity(user : RDD[ItemPref], stype : String) : RDD[ItemSimilar] = {
    val similar = stype match{
      case "cooccurrence" => ItemSimilarity.CooccurenceSimilarity(user) // 同现相似度
      //case "cosine" => // 余弦相似度
      //case "euclidean" => // 欧式距离相似度
      case _ => ItemSimilarity.CooccurenceSimilarity(user)
    }
    similar
  }
}

object ItemSimilarity{
  def CooccurenceSimilarity(user : RDD[ItemPref]) : (RDD[ItemSimilar]) = {
    val user_1 = user.map(r => (r.userid, r.itemid, r.pref)).map(r => (r._1, r._2))
    /**
      * 内连接，默认根据第一个相同字段为连接条件，物品与物品的组合
      */
    val user_2 = user_1.join(user_1)

    /**
      * 统计
      */
    val user_3 = user_2.map(r => (r._2, 1)).reduceByKey(_+_)

    /**
      * 对角矩阵
      */
    val user_4 = user_3.filter(r => r._1._1 == r._1._2)

    /**
      * 非对角矩阵
      */
    val user_5 = user_3.filter(r => r._1._1 != r._1._2)

    /**
      * 计算相似度
      */
    val user_6 = user_5.map(r => (r._1._1, (r._1._1,r._1._2,r._2)))
      .join(user_4.map(r => (r._1._1, r._2)))

    val user_7 = user_6.map(r => (r._2._1._2, (r._2._1._1, r._2._1._2, r._2._1._3, r._2._2)))
      .join(user_4.map(r => (r._1._1, r._2)))

    val user_8 = user_7.map(r => (r._2._1._1, r._2._1._2, r._2._1._3, r._2._1._4, r._2._2))
      .map(r => (r._1, r._2, (r._3 / sqrt(r._4 * r._5))))

    user_8.map(r => ItemSimilar(r._1, r._2, r._3))
  }
}

class RecommendItem{
  def Recommend(items : RDD[ItemSimilar], users : RDD[ItemPref], number : Int) : RDD[UserRecommend] = {
    val items_1 = items.map(r => (r.itemid_1, r.itemid_2, r.similar))
    val users_1 = users.map(r => (r.userid, r.itemid, r.pref))

    /**
      * i行与j列join
      */
    val items_2 = items_1.map(r => (r._1, (r._2, r._3))).join(users_1.map(r => (r._2, (r._1, r._3))))

    /**
      * i行与j列相乘
      */
    val items_3 = items_2.map(r => ((r._2._2._1, r._2._1._1), r._2._2._2 * r._2._1._2))

    /**
      * 累加求和
      */
    val items_4 = items_3.reduceByKey(_+_)

    /**
      * 过滤已存在的物品
      */
    val items_5 = items_4.leftOuterJoin(users_1.map(r => ((r._1, r._2), 1))).filter(r => r._2._2.isEmpty)
      .map(r => (r._1._1, (r._1._2, r._2._1)))

    /**
      * 分组
      */
    val items_6 = items_5.groupByKey()

    val items_7 = items_6.map(r => {
      val i_2 = r._2.toBuffer
      val i_2_2 = i_2.sortBy(_._2)
      if(i_2_2.length > number){
        i_2_2.remove(0, (i_2_2.length - number))
      }
      (r._1, i_2_2.toIterable)
    })

    val items_8 = items_7.flatMap(r => {
      val i_2 = r._2
      for(v <- i_2) yield (r._1, v._1, v._2)
    })

    items_8.map(r => UserRecommend(r._1, r._2, r._3))
  }
}

/**
  * Created by zhen on 2019/8/9.
  */
object ItemCF {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    conf.setAppName("ItemCF")
    conf.setMaster("local[2]")

    val sc = new SparkContext(conf)

    /**
      * 设置日志级别
      */
    Logger.getRootLogger.setLevel(Level.WARN)

    val array = Array("1,1,0", "1,2,1", "1,4,1", "2,1,0", "2,3,1", "2,4,0", "3,1,0", "3,2,1", "4,1,0", "4,3,1")
    val cf = sc.parallelize(array)

    val user_data = cf.map(_.split(",")).map(r => (ItemPref(r(0), r(1), r(2).toDouble)))

    /**
      * 建立模型
      */
    val mySimilarity = new ItemSimilarity()
    val similarity = mySimilarity.Similarity(user_data, "cooccurrence")

    val recommend = new RecommendItem()
    val recommend_rdd = recommend.Recommend(similarity, user_data, 30)

    /**
      * 打印结果
      */
    println("物品相似度矩阵:" + similarity.count())
    similarity.collect().foreach(record => {
      println(record.itemid_1 +","+ record.itemid_2 +","+ record.similar)
    })

    println("用户推荐列表:" + recommend_rdd.count())
    recommend_rdd.collect().foreach(record => {
      println(record.userid +","+ record.itemid +","+ record.pref)
    })
  }
}