package aas

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/6/1211:36
  */
object Defs {

  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setAppName("aas").setMaster("local[1]").setExecutorEnv("executor-memory","6G")

    var sc = new SparkContext(sparkConf)
//    val rdd = sc.textFile("hdfs://192.168.75.83:8020/usr/clyang/block_1.csv")
        val rdd = sc.textFile("E:\\clyang\\资料\\学习资料\\aas文件\\block_1.csv")
    val noHeaders = rdd.filter(!_.contains("id_1"))
    val parsedData = noHeaders.map(parse).repartition(3)
    /*val grouped = parsedData.groupBy(_.matched)
    grouped.mapValues(_.size).foreach(println)*/
    /*val matchCounts = parsedData.map(_.matched).countByValue()

    val matchCountsSeq = matchCounts.toSeq
    matchCountsSeq.sortBy(_._1).reverse.foreach(println)
    matchCountsSeq.sortBy(_._2).reverse.foreach(println)*/
//    val counter = parsedData.map(_.array(0)).stats()
   /* val  counter = parsedData.map(_.array(0)).filter(!_.isNaN).stats()
    println(counter)*/
    /*val nasParsed = parsedData.map(_.array.map(NAStateCounter(_)))
    val reduced = nasParsed.reduce((n1,n2)=>{
      n1.zip(n2).map{case(a,b)=>a.merge(b)}
    })
    reduced.foreach(println)*/
    val statsM = statsWithMissing(parsedData.filter(_.matched).map(_.array))

    //statsM.foreach(println)
    println("--------------------------")
    val statsN = statsWithMissing(parsedData.filter(!_.matched).map(_.array))
    //statsN.foreach(println)
    //进去数据分析
    /*statsM.zip(statsN).map{
      case(m,n)=>(m.missing+n.missing,m.status.mean-n.status.mean)
    }.foreach(println)*/

    //获取价值大的数据进去取值计算  对于差异小 丢失数据多的可以忽略
    def naz(d:Double)=if(Double.NaN.equals(d)) 0.0 else d

    case class  Scored(md:MatchData,score: Double)
    val ct = parsedData.map(md=>{
      val score = Array(2,5,6,7,8).map(i=>naz(md.array(i))).sum
      Scored(md,score)
    })

    /**
      * 总的
      * (false,572820)
      * (true,2093)
      *
      * 3.0
      * (false,31510)
      * (true,2092)
      *4.0
      * (false,66)
      * (true,2087)
      *
      * 1.8
      * (false,63317)
      * (true,2093)
      *
      * 1.9
      * (false,59766)
      * (true,2093)
      * 2.0
      * (false,59729)
      * (true,2093)
      */
    ct.filter(_.score>=2.0).map(_.md.matched).countByValue().foreach(println(_))



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
  }

  def statsWithMissing(rdd:RDD[Array[Double]]):Array[NAStateCounter]={
    val nastats = rdd.mapPartitions((iter:Iterator[Array[Double]])=>{
      val nas:Array[NAStateCounter]=iter.next().map(NAStateCounter(_))
      iter.foreach(arr=>{
        nas.zip(arr).foreach{case(n,d)=>n.add(d)
        }
      })
      Iterator(nas)
    })
    nastats.reduce((n1,n2)=>{
      n1.zip(n2).map{case(a,b)=>a.merge(b)}
    })
  }

}

case class MatchData(id1: Int,id2:Int,array: Array[Double],matched: Boolean){}
