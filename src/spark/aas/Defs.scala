package aas

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/6/1211:36
  */
object Defs {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("aas").setMaster("local[1]")
    var sc = new SparkContext(sparkConf)
    val rdd = sc.textFile("hdfs://192.168.75.83:8020/usr/clyang/block_1.csv")
//    val rdd = sc.textFile("E:\\clyang\\资料\\学习资料\\aas文件\\block_1.csv")
    val noHeaders = rdd.filter(!_.contains("id_1"))
    val parsedData = noHeaders.map(parse).cache()
    parsedData.take(10).foreach(println)
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
