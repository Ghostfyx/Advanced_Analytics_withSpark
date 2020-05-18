package com.spark.learn

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}

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

object RunIntro {
  def main(args: Array[String]): Unit = {
    var spark = SparkSession.builder().master("local[*]").appName("chap02").getOrCreate()
    var sc = SparkContext.getOrCreate()
    val preview = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///Users/yuexiangfan/coding/bigData/Advanced_Analytics_withSpark/data/donation/block_1.csv")
    preview.printSchema()
    preview.show()

    val parsed = spark.read
      .option("header", "true")
      .option("nullValue", "?")
      .option("inferSchema", "true")
      .csv("/Users/yuexiangfan/coding/bigData/AdvancedAnalyticswithSpark/data/donation/block_1.csv")
    parsed.first()
    parsed.take(5)
    parsed.collect()
    val rawblocks = sc.textFile("/Users/yuexiangfan/coding/bigData/AdvancedAnalyticswithSpark/data/donation/block_1.csv")
    val head = rawblocks.take(10)
    head.foreach(println)
    head.filter(isHeader).foreach(println)
    // Scala匿名函数
    head.filter(x => !isHeader(x))
    head.filter(!isHeader(_))
  }
  
  def isHeader(line:String) = line.contains("id_1")

  /**
    * scala函数使用def定义
    * @param line
    * @return
    */
  def isHeader2(line:String):Boolean = {
    line.contains("id_1")
  }
}
