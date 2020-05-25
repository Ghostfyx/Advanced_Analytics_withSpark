package com.spark.learn

import breeze.linalg.{max, min}
import org.apache.spark.sql.{Dataset, SparkSession}

object RunRecommender {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").appName("DataFrameApiDemo").getOrCreate()
    val base = "/Users/yuexiangfan/coding/bigData/Advanced_Analytics_withSpark/data/profiledata_06-May-2005/"
    val rawUserArtistData = spark.read.textFile(base + "user_artist_data.txt")
    val rawArtistData = spark.read.textFile(base + "artist_data.txt")
    val rawArtistAlias = spark.read.textFile(base+"artist_alias.txt")

    val runRecommender = new RunRecommender(spark)
    runRecommender.preparation(rawUserArtistData, rawArtistData, rawArtistAlias)
  }

}

class RunRecommender(private val spark: SparkSession) {
  import spark.implicits._

  def preparation(rawUserArtistData: Dataset[String],
                  rawArtistData: Dataset[String],
                  rawArtistAlias: Dataset[String]):Unit={
//    rawUserArtistData.show(5)
    val userArtistDf = rawUserArtistData.map(line => {
      val Array(user, artist, count) = line.split(' ')
      (user.toInt, artist.toInt, count.toInt)
    }).toDF("userId", "artist", "count")
    userArtistDf.show(5)
    userArtistDf.agg(min("user"), max("user"), min("artist"), max("artist")).show()

  }
}
