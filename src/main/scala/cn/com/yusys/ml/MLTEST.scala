package cn.com.yusys.ml
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.sql.SparkSession

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/12/12
  */
object MLTEST {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession.builder().appName("rddTest").master("local[*]").getOrCreate()
    val sc =  spark.sparkContext
    val spam = sc.textFile("/Users/mac/Documents/workspace/sparkDemo/src/main/scala/cn/com/yusys/ml/spam.txt")
    val normal = sc.textFile("/Users/mac/Documents/workspace/sparkDemo/src/main/scala/cn/com/yusys/ml/ham.txt")
    // 创建一个HashingTF实例来把邮件文本映射为包含10000个特征的向量
    val tf = new HashingTF(numFeatures = 10000)
    // 各邮件都被切分为单词，每个单词被映射为一个特征
    val spamFeatures = spam.map(email => tf.transform(email.split(" ")))
    val normalFeatures = normal.map(email => tf.transform(email.split(" ")))
    // 创建LabeledPoint数据集分别存放阳性(垃圾邮件)和阴性(正常邮件)的例子
    val positiveExamples = spamFeatures.map(features => LabeledPoint(1, features))
    val negativeExamples = normalFeatures.map(features => LabeledPoint(0, features))
    val trainingData = positiveExamples.union(negativeExamples)
    trainingData.cache() // 因为逻辑回归是迭代算法，所以缓存训练数据RDD
    // 使用SGD算法运行逻辑回归
    val model = new LogisticRegressionWithSGD().run(trainingData)
    // 以阳性(垃圾邮件)和阴性(正常邮件)的例子分别进行测试
    val posTest = tf.transform(
    "O M G GET cheap stuff by sending money to ...".split(" "))
    val negTest = tf.transform(
      "Hi Dad, I started studying Spark the other ...".split(" "))
    println("Prediction for positive test example: " + model.predict(posTest))
    println("Prediction for negative test example: " + model.predict(negTest))

  }

}
