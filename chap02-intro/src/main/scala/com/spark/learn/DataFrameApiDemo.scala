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

object DataFrameApiDemo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DataFrameApiDemo").getOrCreate()
    val parsed = spark.read
      .option("header", "true")
      .option("nullValue", "?")
      .option("inferSchema", "true")
      .csv("/Users/yuexiangfan/coding/bigData/Advanced_Analytics_withSpark/data/donation/block_1.csv")
    parsed.count()
    parsed.cache()
    parsed.groupBy("is_match").count().orderBy("count").show()
    // 创建临时表，使用spark SQL
    parsed.createOrReplaceTempView("linkage")
    spark.sql("""
      SELECT is_match, COUNT(*) cnt
      FROM linkage
      GROUP BY is_match
      ORDER BY cnt DESC
    """).show()

  }
}
