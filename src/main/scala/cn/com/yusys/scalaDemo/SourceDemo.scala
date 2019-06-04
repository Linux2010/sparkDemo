package cn.com.yusys.scalaDemo
import scala.io.Source
import scala.sys.process._
object SourceDemo  {
  def main(args: Array[String]): Unit = {

    val source = Source.fromFile("/Users/mac/Documents/workspace/sparkDemo/src/main/resources/1.txt","UTF-8")
    val lineIterator = source.getLines()
    for(l <- lineIterator) println(l)
    "ls -al ..".!

  }
}
