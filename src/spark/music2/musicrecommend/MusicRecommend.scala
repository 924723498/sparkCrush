package spark.music2.musicrecommend

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.Random

/**
  *
  * @ClassName MusicRecommend
  * @Description MusicRecommend
  * @Author Administrator
  * @Date 2020/10/20 14:19
  *
  */
object MusicRecommend {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().config("spark.sql.crossJoin.enabled", "true").getOrCreate()
    spark.sparkContext.setCheckpointDir("tmp")
    val base = "E:\\clyang\\资料\\学习资料\\第三章数据\\profiledata_06-May-2005\\"
    val rawUserArtistData = spark.read.textFile(base+"user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base+"artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base +"artist_alias.txt")
    val musicRecommend = new MusicRecommend(spark)
    // musicRecommend.preparation(rawUserArtistData,rawArtistData,rawArtistAlias)
    musicRecommend.model(rawUserArtistData,rawArtistData,rawArtistAlias)
  }

}

class MusicRecommend(private val spark:SparkSession){
  import spark.implicits._

  /**
    * 数据调试 准备
    * @param rawUserArtistData
    * @param rawArtistData
    * @param rawArtistAlias
    */
  def preparation( rawUserArtistData: Dataset[String],
                   rawArtistData: Dataset[String],
                  rawArtistAlias: Dataset[String]
                 ): Unit ={

    rawUserArtistData.take(5).foreach(println)

    val userArtistDF = rawUserArtistData.map{line =>
      val Array(user,artist,_*) =line.split(" ")
      (user.toInt,artist.toInt)
    }.toDF("user","artist")

    userArtistDF.agg(min("user"),max("user"),min("artist"),max("artist")).show()
    val artistById = buildArtistByID(rawArtistData)
    val artistAlias = buildArtistAlias(rawArtistAlias)

    val (badId,goodId) = artistAlias.head
    artistById.filter($"id" isin(badId,goodId)).show()
  }

  def buildCounts(rawUserArtistData: Dataset[String], bArtistAlias: Broadcast[Map[Int, Int]]) = {
    rawUserArtistData.map{line=>
      val Array(userId,artistId,count) = line.split(' ').map(_.toInt)
      val finalArtistId = bArtistAlias.value.getOrElse(artistId,artistId)
      (userId,finalArtistId,count)
    }.toDF("user","artist","count")
  }

  def makeRecommendations(model: ALSModel, userId: Int, howMany: Int):DataFrame= {
    val toRecommend = model.itemFactors.
      select($"id".as("artist")).
      withColumn("user", lit(userId))
    model.transform(toRecommend).
      select("artist", "prediction").
      orderBy($"prediction".desc).
      limit(howMany)



  }



  def model(
             rawUserArtistData: Dataset[String],
             rawArtistData: Dataset[String],
             rawArtistAlias: Dataset[String]
           ): Unit ={
    val bArtistAlias = spark.sparkContext.broadcast(buildArtistAlias(rawArtistAlias))
    val trianData = buildCounts(rawUserArtistData,bArtistAlias).cache()
    val model = new ALS().
      setSeed(Random.nextLong()).
      setImplicitPrefs(true).
      setRank(10).
      setRegParam(0.01).
      setAlpha(1.0).
      setMaxIter(5).
      setUserCol("user").
      setItemCol("artist").
      setRatingCol("count").
      setPredictionCol("prediction").
      fit(trianData)
    trianData.unpersist()

    model.userFactors.select("features").show(truncate = false)

    val userId = 2093760

    val existingArtistIds = trianData.filter($"user" ===userId).
      select("artist").as[Int].collect()
    val artistById = buildArtistByID(rawArtistData)

    artistById.filter($"id"  isin (existingArtistIds:_*) ).show()

    val topRecommendations = makeRecommendations(model,userId,5)
    topRecommendations.show()
    val recommendedArtistIds = topRecommendations.select("artist").as[Int].collect()
    artistById.filter($"id" isin(recommendedArtistIds:_*)).show()


    model.userFactors.unpersist()
    model.itemFactors.unpersist()








  }



  def buildArtistByID(rawArtistData:Dataset[String]): DataFrame ={
    rawArtistData.flatMap{line =>
      val (id,name) = line.span(_ != '\t')
      if(name.isEmpty){
        None
      }else{
        try{
          Some((id.toInt,name.trim))
        } catch{
          case _: NumberFormatException =>None
        }
      }
    }.toDF("id","name")
  }

  def buildArtistAlias(rawArtistAlias:Dataset[String]):Map[Int,Int] ={
    rawArtistAlias.flatMap{ line =>
      val Array(artist,alias) = line.split('\t')
      if(artist.isEmpty){
        None
      }else{
        Some((artist.toInt,alias.toInt))
      }
    }.collect().toMap
  }

}
