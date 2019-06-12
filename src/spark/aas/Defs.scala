package aas

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/6/1211:36
  */
object Defs {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("aas").setMaster("local[2]")
    var sc = new SparkContext(sparkConf)
    sc.setLogLevel("DEBUG")
//    val rdd = sc.textFile("hdfs://192.168.75.83:8020/usr/clyang/block_1.csv")
    val rdd = sc.textFile("E:\\clyang\\资料\\学习资料\\aas文件\\block_1.csv")
    val header =rdd.take(1)
    header.foreach(println)
  }

  def toDouble(input:String): Double ={
    if(input.equals("?")){
      Double.NaN
    }else{
      input.toDouble
    }
  }

  def parse(line :String)={
    val picies = line.split(",")
    val id1 =picies(0).toInt
    val id2 = picies(1).toInt
    val scores = picies.slice(2,11).map(toDouble)
    val matchs = picies(11).toBoolean
    MatchData(id1,id2,scores,matchs)
//    (id1,id2,scores,matchs)
  }

}

case class MatchData(id1: Int,id2:Int,array: Array[Double],boolean: Boolean){}
