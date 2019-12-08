package cn.com.yusys

import java.util

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

import scala.collection.mutable

/**
  * @author tianfusheng
  * @e-mail linuxmorebetter@gmail.com
  * @date 2019/12/5
  */
object rddTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("rddTest").master("local[*]").getOrCreate()
    val sc = spark.sparkContext
    val rdd1 = sc.parallelize(List("123b", "324b", "3333b", "sdfadf", "5fdasdf w", "123a", "324as", "3333a", "sdfadf", "5fdasdf w", "123a", "324as", "3333a", "sdfadf", "5fdasdf w"))
    val rdd2 = sc.parallelize(List("123a", "324as", "3333a", "sdfadf", "5fdasdf w", "123sss", "324as", "3333a", "sdfadf", "5fdasdf w", "123a", "324as", "3333a", "sdfadf", "5fdasdf w"))


    val rddpair1 = rdd1.groupBy(s => s.length % 12)

    val rddpair2 = rdd2.groupBy(s => s.length % 12)


    val rddd = rddpair1.cogroup(rddpair2)


    val ressrdd = rddd.map(t=>{
      val res =t._2._1.map(s => {
        val res2 = s.map(
          ss => {
            var flag = false
            t._2._2.foreach(x => {
              x.foreach(xx => {
                if (xx.length == ss.length)
                  if (xx.equals(ss)) {
                    flag = true
                  }
              })
            })
            if (flag equals false) {
              println(flag)
              ss
            }
          }
        )
        res2
      })
      res
    })

    ressrdd.foreach(s=>println(s))



    rddd.cache()

//    val resRDD= rddd.mapPartitions(x=>{
//      val res=x.map(k=>{
//        val res1 = k._2._1.map(s => {
//          val res2 = s.map(
//            ss => {
//              var flag = false
//              k._2._2.foreach(x => {
//                x.foreach(xx => {
//                  if (xx.length == ss.length)
//                    if (xx.equals(ss)) {
//                      flag = true
//                    }
//                })
//              })
//              if (flag equals false) {
//                println(flag)
//                ss
//              }
//            }
//          )
//          res2
//        })
//        res1
//      })
//      res
//    })

//    resRDD.foreach(s=>println(s))




    //var i = 0



//    rddd.foreachPartition(x => {
//      while (x.hasNext) {
//        val t = x.next()
//
//        t._2._1.foreach(s => println(s))
//        t._2._2.foreach(x => println(x))
//
//        println("-----------------------")
//
//        t._2._1.foreach(s => {
//          s.foreach(
//            ss => {
//              var flag = false
//              t._2._2.foreach(x => {
//                x.foreach(xx => {
//                  if (xx.length == ss.length)
//                    if (xx.equals(ss)) {
//                      i = i + 1
//                      flag = true
//                    }
//                })
//              })
//              if (flag equals false) {
//                println(flag)
//              }
//            }
//          )
//        })
//
//        t._2._2.foreach(x => {
//          x.foreach(xx => {
//            var flag = false
//            t._2._1.foreach(s => {
//              s.foreach(ss => {
//                if (xx.length == ss.length)
//                  if (xx.equals(ss)) {
//                    i = i + 1
//                    flag = true
//                  }
//              })
//            })
//            if (flag equals false) {
//              println(flag)
//            }
//          })
//        })
//
//        println("count::" + i)
//
//      }
//    })



  }

  /*  val count = rdd1.count

    if (rdd1.count < 2) {
      sc.makeRDD[(String, String)](Seq.empty)
    } else if (rdd1.count == 2) {
      val values = rdd1.collect
      sc.makeRDD[(String, String)](Seq((values(0), values(1))))
    } else {
      val elem = rdd1.take(1)
      val elemRdd = sc.makeRDD(elem)
      val subtracted = rdd1.subtract(elemRdd)
      val comb = subtracted.map(e => (elem(0), e))
      //      comb.union(combs(subtracted))
    }*/


}
