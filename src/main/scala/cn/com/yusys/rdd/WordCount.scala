package cn.com.yusys.rdd

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args:Array[String]):Unit={
    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(conf)
    val text = sc.textFile("src/main/resources/1.txt")
    val words = text.flatMap(line =>line.split(","))//？
    val pairs = words.map(word =>(word,1))//?
    val result = pairs.reduceByKey(_+_)
    val sorted = result.sortByKey(false);
    sorted.foreach(x => println(x));
  }
}
