package cn.com.yusys.itemCF

import scala.math._
import org.apache.spark.rdd.RDD


/**
  * 物品相似度计算类
  * 通过设置模型参数后执行Similarity方法，进行相似度计算，返回物品与物品相似度RDD
  * 相似度计算支持：同现相似度、余弦相似度、欧氏距离相似度
  *
  */

/**
  * 用户评分
  *
  * @param userid 用户
  * @param itemid 评分物品
  * @param pref   评分
  */
case class ItemPref(
                     val userid: String,
                     val itemid: String,
                     val pref: Double
                   ) extends Serializable

/**
  * 用户推荐
  *
  * @param userid 用户
  * @param itemid 推荐物品
  * @param pref   评分
  */
case class UserRecomm(
                       val userid: String,
                       val itemid: String,
                       val pref: Double
                     ) extends Serializable

/**
  * 相似度
  *
  * @param itemid1 物品
  * @param itemid2 物品
  * @param similar 相似度
  */
case class ItemSimi(
                     val itemid1: String,
                     val itemid2: String,
                     val similar: Double
                   ) extends Serializable

/**
  * 相似度计算
  * 支持同现相似度、余弦相似度、欧氏距离相似度
  */
class ItemSimilarity extends Serializable {

  /**
    * 相似度计算
    *
    * @param user_rdd 用户评分
    * @param stype    计算相似度方式
    * @return 返回物品相似度
    */
  def Similarity(user_rdd: RDD[ItemPref], stype: String): (RDD[ItemSimi]) = {
    val simil_rdd = stype match {
      case "cooccurrence" => ItemSimilarity.CooccurrenceSimilarity(user_rdd)
      case "cosine" => ItemSimilarity.CosineeSimilarity(user_rdd)
      case "euclidean" => ItemSimilarity.EuclideanDistanceSimilarity(user_rdd)
      case _ => ItemSimilarity.CooccurrenceSimilarity(user_rdd)
    }
    simil_rdd
  }

}

object ItemSimilarity {
  /**
    * 同现相似度矩阵计算
    * w(i,j)=N(i)∩N(j)/sqrt(N(i)*N(j))
    *
    * @param user_rdd 用户评分
    * @return 返回物品相似度
    */
  def CooccurrenceSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    //1.数据准备
    val user_rdd1: RDD[(String, String, Double)] = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2: RDD[(String, String)] = user_rdd1.map(f => (f._1, f._2))
    //2.(用户，物品)笛卡尔积操作=>物品与物品组合
    val user_rdd3: RDD[(String, (String, String))] = user_rdd2.join(user_rdd2)
    val user_rdd4: RDD[((String, String), Int)] = user_rdd3.map(f => (f._2, 1))
    //3.(物品，物品，频次)
    val user_rdd5: RDD[((String, String), Int)] = user_rdd4.reduceByKey((x, y) => x + y)
    //4.对角矩阵
    val user_rdd6: RDD[((String, String), Int)] = user_rdd5.filter(f => f._1._1 == f._1._2)
    //5.非对角矩阵
    val user_rdd7: RDD[((String, String), Int)] = user_rdd5.filter(f => f._1._1 != f._1._2)
    //6.计算同现相似度(物品1，物品2，同现频次)
    val user_rdd8: RDD[(String, ((String, String, Int), Int))] = user_rdd7.
      map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd9: RDD[(String, (String, String, Int, Int))] = user_rdd8.map(f => (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10: RDD[(String, ((String, String, Int, Int), Int))] = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd11: RDD[(String, String, Int, Int, Int)] = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    val user_rdd12: RDD[(String, String, Double)] = user_rdd11.map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))
    //7.结果返回
    user_rdd12.map(f => ItemSimi(f._1, f._2, f._3))
  }

  /**
    * 余弦相似度矩阵计算
    * T(x,y)=∑x(i)y(i)/sqrt(∑(x(i)*y(i))*∑(y(i)*y(i)))
    *
    * @param user_rdd 用户评分
    * @return 返回物品相似度
    */
  def CosineeSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    //1.数据准备
    val user_rdd1: RDD[(String, String, Double)] = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2: RDD[(String, (String, Double))] = user_rdd1.map(f => (f._1, (f._2, f._3)))
    //2.(用户，物品，评分)笛卡尔积操作=>（物品1，物品2，评分1，评分2）组合
    val user_rdd3: RDD[(String, ((String, Double), (String, Double)))] = user_rdd2.join(user_rdd2)
    val user_rdd4: RDD[((String, String), (Double, Double))] = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
    //3.(物品1，物品2，评分1，评分2,)组合=>（物品1，物品2，评分1*评分2）组合并累加
    val user_rdd5: RDD[((String, String), Double)] = user_rdd4.map(f => (f._1, f._2._1 * f._2._2)).reduceByKey(_ + _)
    //4.对角矩阵
    val user_rdd6: RDD[((String, String), Double)] = user_rdd5.filter(f => f._1._1 == f._1._2)
    //5.非对角矩阵
    val user_rdd7: RDD[((String, String), Double)] = user_rdd5.filter(f => f._1._1 != f._1._2)
    //6.计算相似度
    val user_rdd8: RDD[(String, ((String, String, Double), Double))] = user_rdd7.map(f => (f._1._1, (f._1._1, f._1._2, f._2))).join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd9: RDD[(String, (String, String, Double, Double))] = user_rdd8.map(f => (f._2._1._2, (f._2._1._1, f._2._1._2, f._2._1._3, f._2._2)))
    val user_rdd10: RDD[(String, ((String, String, Double, Double), Double))] = user_rdd9.join(user_rdd6.map(f => (f._1._1, f._2)))
    val user_rdd11: RDD[(String, String, Double, Double, Double)] = user_rdd10.map(f => (f._2._1._1, f._2._1._2, f._2._1._3, f._2._1._4, f._2._2))
    val user_rdd12: RDD[(String, String, Double)] = user_rdd11.map(f => (f._1, f._2, (f._3 / sqrt(f._4 * f._5))))
    //7.结果返回
    user_rdd12.map(f => ItemSimi(f._1, f._2, f._3))
  }

  /**
    * 欧氏距离相似度矩阵计算
    * d(x,y)=sqrt(∑((x(i)-y(i))*(x(i)-y(i))))
    * sim(x,y)=n/(1+d(x,y))
    *
    * @param user_rdd 用户评分
    * @return 返回物品相似度
    */
  def EuclideanDistanceSimilarity(user_rdd: RDD[ItemPref]): (RDD[ItemSimi]) = {
    //1.数据准备
    val user_rdd1: RDD[(String, String, Double)] = user_rdd.map(f => (f.userid, f.itemid, f.pref))
    val user_rdd2: RDD[(String, (String, Double))] = user_rdd1.map(f => (f._1, (f._2, f._3)))
    //2.(用户，物品，评分)笛卡尔积操作=>（物品1，物品2，评分1，评分2）组合
    val user_rdd3: RDD[(String, ((String, Double), (String, Double)))] = user_rdd2 join user_rdd2
    val user_rdd4: RDD[((String, String), (Double, Double))] = user_rdd3.map(f => ((f._2._1._1, f._2._2._1), (f._2._1._2, f._2._2._2)))
    //3.(物品1，物品2，评分1，评分2)组合=>（物品1，物品2，评分1-评分2）组合并累加
    val user_rdd5: RDD[((String, String), Double)] = user_rdd4.map(f => (f._1, (f._2._1 - f._2._2) * (f._2._1 - f._2._2))).reduceByKey(_ + _)
    //4.(物品1，物品2，评分1，评分2)组合=>（物品1，物品2,1）组合计算物品1和物品2的重叠度
    val user_rdd6: RDD[((String, String), Int)] = user_rdd4.map(f => (f._1, 1)).reduceByKey(_ + _)
    //5.非对角矩阵
    val user_rdd7: RDD[((String, String), Double)] = user_rdd5.filter(f => f._1._1 != f._1._2)
    //6.相似度计算
    val user_rdd8: RDD[((String, String), (Double, Int))] = user_rdd7.join(user_rdd6)
    val user_rdd9: RDD[(String, String, Double)] = user_rdd8.map(f => (f._1._1, f._1._2, f._2._2 / (1 + sqrt(f._2._1))))
    //7.结果返回
    user_rdd9.map(f => ItemSimi(f._1, f._2, f._3))
  }


}
