package aas

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/6/1316:48
  */
object ShellModel {
  def main(args: Array[String]): Unit = {
    /*val nas1 = NAStateCounter(10.0)
    nas1.add(1.1)
    val nas2 = NAStateCounter(11.0)
    nas1.merge(nas2)

    print(nas1)

    val arr = Array(1.0,Double.NaN,12.1)

    val nas = arr.map(NAStateCounter(_))
    println(nas)
*/

    val nas11 = Array(1.0,Double.NaN,3.1).map(NAStateCounter(_))
    val nas22 = Array(Double.NaN,2.0,4.4).map(NAStateCounter(_))
    val counters = nas11.zip(nas22)
    counters.map(p => p._1.merge(p._2))
    print(counters)

  }

}
