package cn.com.yusys.scalaDemo

trait  Logger{
  def log(msg:String)
}
class ConsoleLogger extends Logger{
  override def log(msg: String): Unit = println(msg)
}