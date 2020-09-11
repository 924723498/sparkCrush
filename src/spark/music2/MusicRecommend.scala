package music2

import music2.SysConfig.getHdfsPath
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/6/189:45
  */
class MusicRecommend {

}

object MusicRecommend{

  def main(args: Array[String]): Unit = {
    //对运行环境内存进行设置  6G  太小了跑不出来 -Xms12288m -Xmx12288m
//    user_artist_data.txt   artist_data.txt  artist_alias.txt
    //user-artists
    val sc = SysConfig.getSC("music")
    val rawUserArtists =sc.textFile(getHdfsPath()+"/usr/clyang/second/user_artist_data.txt")
   /* val counter: StatCounter = rawUserArtists.mapPartitions((iter: Iterator[String]) => {
      val id = iter.next().split(" ")(0).toDouble
      Iterator(id)
    }).stats()
    println(counter)*/
    //查看id最大值是否操作int最大值 为了后面矩阵计算的值要求进行验证
//    val status =rawUserArtists.map(_.split(' ')(0).toDouble).stats()
//println(status)
    //artists
    val rawArtists = sc.textFile(getHdfsPath()+"/usr/clyang/second/artist_data.txt")
    val artistsById =rawArtists.flatMap{ line=>
      val (id,name)=line.span(_!='\t')
      if(name.isEmpty){
        None
      }else{
        try{
          Some(id.toInt,name.trim)
        }catch {
          case e:NumberFormatException=>None
        }
      }

    }
//    artistsById.lookup(6803336).head.foreach(print)
    //artists-alias
    val rawArtistsAlias = sc.textFile(getHdfsPath()+"/usr/clyang/second/artist_alias.txt")
    val artistsAlias = rawArtistsAlias.flatMap{line =>
      val tokens = line.split('\t')
      if(tokens(0).isEmpty){
        None
      }else{
        Some(tokens(0).toInt,tokens(1).toInt)
      }
    }.collectAsMap()

    val bArtistsAlias = sc.broadcast(artistsAlias)

    /*val trainData = rawUserArtists.map{
      line =>
        val Array(userId,artistsId,count)=line.split(' ').map(_.toInt)
        val finalArtistsId = bArtistsAlias.value.getOrElse(artistsId,artistsId)
        Rating(userId,finalArtistsId,count)

    }.cache()

//    val model = ALS.trainImplicit(trainData,10,5,0.01,1.0)
//    model.save(sc,"mode-music-als")
    val model = MatrixFactorizationModel.load(sc,"mode-music-als")
//    val tuple = model.userFeatures.mapValues(_.mkString(",")).first()
    val recomended = model.recommendProducts(2093760,5)
    recomended.foreach(println(_))

    val recomendedProductIds = recomended.map(_.product).toSet
    artistsById.filter{
      case(id,name)=>
        recomendedProductIds.contains(id)
    }.values.collect().foreach(println)
*/

    /*
     * (90,-0.840640664100647,-0.1471276879310608,-0.4095822870731354,-0.06496176868677139,
     * -0.016586780548095703,0.3964614272117615,0.2393215298652649,1.145279049873352,
     * -0.30688583850860596,0.006257241126149893)
    */
//    println(tuple)
    /*
    获取对应人听过歌曲的人
    David Gray
    Blackalicious
    Jurassic 5
    The Saw Doctors
    Xzibit
     */
    /*val rawUserArtistsforUser = rawUserArtists.map(_.split(' ')).filter{

      case Array(user,_,_)=> user.toInt==2093760
    }

    val existsProducts = rawUserArtistsforUser.map{case Array(_,artist,_)=>artist.toInt}.collect().toSet

    artistsById.filter{case (id,name)=>existsProducts.contains(id)}
      .values.collect().foreach(println)*/
    RunRecommender.evaluate(sc,rawUserArtists,rawArtistsAlias)


  }
}
