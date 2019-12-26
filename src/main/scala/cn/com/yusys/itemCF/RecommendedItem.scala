package cn.com.yusys.itemCF

import org.apache.spark.rdd.RDD

import scala.collection.mutable

/**
  * 物品推荐计算类
  * 通过设置模型参数，执行Recommend方法进行推荐计算，返回用户的推荐物品RDD
  * 推荐计算根据物品相似度和用户评分进行推荐物品计算，并过滤用户已有物品及过滤最大过滤推荐数量
  */
class RecommendedItem {

  /**
    * 用户推荐计算
    *
    * @param items_similar 物品相似度
    * @param user_prefer   用户评分
    * @param r_number      推荐数量
    * @return 返回用户推荐物品
    */
  def Recommend(items_similar: RDD[ItemSimi], user_prefer: RDD[ItemPref], r_number: Int): (RDD[UserRecomm]) = {
    //1.数据准备
    val rdd_app1_R1: RDD[(String, String, Double)] = items_similar.map(f => (f.itemid1, f.itemid2, f.similar))
    val user_prefer1: RDD[(String, String, Double)] = user_prefer.map(f => (f.userid, f.itemid, f.pref))
    //2.矩阵计算（i行j列join）
    val rdd_app1_R2: RDD[(String, ((String, Double), (String, Double)))] = rdd_app1_R1.map(f => (f._1, (f._2, f._3))).join(user_prefer1.map(f => (f._2, (f._1, f._3))))
    //3.矩阵计算（i行j列相乘）
    val rdd_app1_R3: RDD[((String, String), Double)] = rdd_app1_R2.map(f => ((f._2._2._1, f._2._1._1), f._2._2._2 * f._2._1._2))
    //4.矩阵计算（用户：元素累加求和）
    val rdd_app1_R4: RDD[((String, String), Double)] = rdd_app1_R3.reduceByKey((x, y) => x + y)
    //5.矩阵计算（用户：对结果过滤已有物品）
    val rdd_app1_R5: RDD[(String, (String, Double))] = rdd_app1_R4.leftOuterJoin(user_prefer1.map(f => ((f._1, f._2), 1))).filter(f => f._2._2.isEmpty).map(f => (f._1._1, (f._1._2, f._2._1)))
    //6.矩阵计算（用户：用户对结果排序，过滤）
    val rdd_app1_R6: RDD[(String, Iterable[(String, Double)])] = rdd_app1_R5.groupByKey()
    val rdd_app1_R7: RDD[(String, Iterable[(String, Double)])] = rdd_app1_R6.map(f => {
      val i2: mutable.Buffer[(String, Double)] = f._2.toBuffer
      val i2_2: mutable.Buffer[(String, Double)] = i2.sortBy(_._2)
      if (i2_2.length > r_number) i2_2.remove(0, (i2_2.length - r_number))
      (f._1, i2_2.toIterable)
    })
    val rdd_app1_R8: RDD[(String, String, Double)] = rdd_app1_R7.flatMap(f => {
      val id2: Iterable[(String, Double)] = f._2
      for (w <- id2) yield (f._1, w._1, w._2)
    })
    rdd_app1_R8.map(f => UserRecomm(f._1, f._2, f._3))
  }

  /**
    * 用户推荐计算
    *
    * @param items_similar 物品相似度
    * @param user_prefer   用户评分
    * @return 返回用户推荐物品
    */
  def Recommend(items_similar: RDD[ItemSimi], user_prefer: RDD[ItemPref]): (RDD[UserRecomm]) = {
    //1.数据准备
    val rdd_app1_R1: RDD[(String, String, Double)] = items_similar.map(f => (f.itemid1, f.itemid2, f.similar))
    val user_prefer1: RDD[(String, String, Double)] = user_prefer.map(f => (f.userid, f.itemid, f.pref))
    //2.矩阵计算（i行和j列join）
    val rdd_app1_R2: RDD[(String, ((String, Double), (String, Double)))] = rdd_app1_R1.map(f => (f._1, (f._2, f._3))).join(user_prefer1.map(f => (f._2, (f._1, f._3))))
    //3.矩阵计算（i行j列元素相乘）
    val rdd_app1_R3: RDD[((String, String), Double)] = rdd_app1_R2.map(f => ((f._2._2._1, f._2._1._1), f._2._2._2 * f._2._1._2))
    //4.矩阵计算（用户：元素累加求和）
    val rdd_app1_R4: RDD[((String, String), Double)] = rdd_app1_R3.reduceByKey((x, y) => x + y)
    //5.矩阵计算（用户：对结果过滤已有物品）
    val rdd_app1_R5: RDD[(String, (String, Double))] = rdd_app1_R4.leftOuterJoin(user_prefer1.map(f => ((f._1, f._2), 1))).filter(f => f._2._2.isEmpty).map(f => (f._1._1, (f._1._2, f._2._1)))
    //6.矩阵计算（用户：用户对结果排序，过滤）
    val rdd_app1_R6: RDD[(String, String, Double)] = rdd_app1_R5.map(f => (f._1, f._2._1, f._2._2)).sortBy(f => (f._1, f._3))
    rdd_app1_R6.map(f => UserRecomm(f._1, f._2, f._3))
  }

}
