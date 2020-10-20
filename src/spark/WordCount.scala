import breeze.linalg.{DenseVector, Vector}
import java.util.{Properties, Random}

import scala.util.Properties

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/5/2911:29
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val prop = new Properties()

    prop.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("application.properties"))

    print(prop.get("a"))

  }


}
