package spark.util

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties

object PropUtils {
  val properties = new Properties();
  properties.load(new BufferedReader(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream("resource/application.properties"),"utf-8")))

  def readProp(key:String)={
    properties.get(key)
  }

  def main(args: Array[String]): Unit = {
    print(readProp("music.base.url"))
  }


}
