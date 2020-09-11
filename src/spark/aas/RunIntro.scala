package aas

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions._

/**
  * @projectName sparkCrushing
  * @author CLYANG
  * @date 2019/6/2010:26
  *       参考 github   获取其编程风格
  */

/**
  * 为数据定义实体类
  * */
case class MatchData(
                      id_1: Int,
                      id_2: Int,
                      cmp_fname_c1: Option[Double],
                      cmp_fname_c2: Option[Double],
                      cmp_lname_c1: Option[Double],
                      cmp_lname_c2: Option[Double],
                      cmp_sex: Option[Int],
                      cmp_bd: Option[Int],
                      cmp_bm: Option[Int],
                      cmp_by: Option[Int],
                      cmp_plz: Option[Int],
                      is_match: Boolean
                    )

object RunIntro extends Serializable {



  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("Intro").master("local[4]").getOrCreate
    import  spark.implicits._
    /*val preView = spark.read.csv("hdfs://192.168.75.83:8020/usr/clyang/block_*.csv")
    preView.show()
    val schema: StructType = preView.schema
    println(schema)*/

    val parsed = spark.read.option("header","true")
      .option("nullValue","?")
      .option("inferSchema","true")
      .csv("hdfs://192.168.75.83:8020/usr/clyang/block_1.csv")

    parsed.show()

    parsed.count()
    parsed.cache()
    parsed.groupBy("is_match").count().orderBy($"count".desc).show()



    parsed.createOrReplaceTempView("linkage")
    spark.sql(
      """
        select is_match,count(1) cnt
        from linkage group by is_match
        order by is_match desc
      """).show()

    val summary = parsed.describe()
    summary.show()
    summary.select("summary","cmp_fname_c1","cmp_fname_c2").show()
    val matchs = parsed.where("is_match=true")
    val misses = parsed.filter($"is_match"===false)
    val matchSummary = matchs.describe()
    val missesSummary = misses.describe()

    val matchSummaryT = pivotSummary(matchSummary)
    val missesSummaryT =pivotSummary(missesSummary)
    matchSummaryT.createOrReplaceTempView("match_desc")
    missesSummaryT.createOrReplaceTempView("miss_desc")
    spark.sql(
      """
        SELECT a.field, a.count + b.count total, a.mean - b.mean delta
              FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
             ORDER BY delta DESC, total DESC
      """).show()

  }

  def pivotSummary(desc: DataFrame) = {
    val lf = longForm(desc)
    lf.groupBy("field").pivot("metric",Seq("count","mean","stddev","min","max")).agg(first("value"))

  }

  def longForm(desc:DataFrame) :DataFrame={
    import desc.sparkSession.implicits._
    val columns = desc.schema.map(_.name)
    desc.flatMap(row=>{
      val metric = row.getAs[String](columns.head)
      columns.tail.map(columnName=>(metric,columnName,row.getAs[String](columnName).toDouble))
    }).toDF("metric","field","value")
  }




}
