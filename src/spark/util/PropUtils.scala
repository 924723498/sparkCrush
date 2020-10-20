package spark.util

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

object PropUtils {
  val properties = new Properties();
  properties.load(new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("application.properties"),"gbk")))

  def readProp(key:String)={
    properties.get(key)
  }

  def main(args: Array[String]): Unit = {
    print(readProp("music.base.url"))
  }


}
