package music2

import org.apache.spark.{SparkConf, SparkContext}

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/6/189:48
  */
object SysConfig {

  def getSC(appNam:String)={
    new SparkContext(new SparkConf().setAppName(appNam).setMaster("local[1]"))
  }

  def getHdfsPath():String={
    "hdfs://192.168.75.83:8020"
  }

  def getHdfsRdd(appNam:String,path:String)={
    val artists = getSC(appNam).textFile(getHdfsPath()+path)
    artists
  }

}
