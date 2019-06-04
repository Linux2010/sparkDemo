package cn.com.yusys.scalaDemo

import java.util.regex.Pattern

import scala.io.Source

/**
  * Created by Administrator on 2017/4/18.
  */
object CrawlerDemo {
    def main(args: Array[String]): Unit = {
//        val url = new URL("http://img.ivsky.com/img/bizhi/pre/201611/29/gulnazar-009.jpg");
//        val conn = url.openConnection();
//        val in = conn.getInputStream
//        val out = new FileOutputStream("d:/scala/1.jpg");
//        var len = 0 ;
//        //创建数组字节
//        val buf = new Array[Byte](1024);
//
//        while((len = in.read(buf)) != -1){
//            out.write(buf,0,len);
//        }
//        out.close()
//        in.close();

        val str = Source.fromFile("d:/scala/1.html").mkString
        val p = Pattern.compile("<a\\s*href=\"([\u0000-\uffff&&[^\u005c\u0022]]*)\"");
        val m = p.matcher(str);
        while (m.find()) {
            val s = m.group(1);
            System.out.println(s);
        }


    }
}
