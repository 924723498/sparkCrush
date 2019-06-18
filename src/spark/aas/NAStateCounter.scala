package aas

import org.apache.spark.util.StatCounter

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/6/1316:35
  */
class NAStateCounter extends Serializable {
  var missing:Long=0
  val status:StatCounter = new StatCounter()

  def add(x:Double):NAStateCounter={
    if(java.lang.Double.isNaN(x)){
      missing += 1
    }else{
      status.merge(x)
    }
    this
  }

  def merge(other:NAStateCounter):NAStateCounter={
    status.merge(other.status)
    missing += other.missing
    this
  }


  override def toString = s"NAStateCounter(missing:$missing, status:$status)"


}
object NAStateCounter extends Serializable{
  def apply(x:Double): NAStateCounter = new NAStateCounter().add(x)

}
