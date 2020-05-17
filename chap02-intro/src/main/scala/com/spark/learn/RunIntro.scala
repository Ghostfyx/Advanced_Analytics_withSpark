package com.spark.learn

import org.apache.spark.sql.SparkSession

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

    val preview = spark.read.csv("/Users/yuexiangfan/coding/bigData/AdvancedAnalyticswithSpark/data/donation/block_1.csv")
    preview.printSchema()
    preview.show()

    val parsed = spark.read
      .option("header", "true")
      .option("nullValue", "?")
      .option("inferSchema", "true")
      .csv("/Users/yuexiangfan/coding/bigData/AdvancedAnalyticswithSpark/data/donation/block_1.csv")
    parsed.show()
    parsed.printSchema()
  }
}
