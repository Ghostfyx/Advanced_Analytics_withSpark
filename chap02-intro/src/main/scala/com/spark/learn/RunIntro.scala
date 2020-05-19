package com.spark.learn

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Row, SparkSession}


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
    // 使用DataFrameReader API读取文件数据
    val jsonDf_1 = spark.read.format("json").load("file.json")
    val josnDf_2 = spark.read.json("file.json")
    // DataFrameWriter API
    val jsonWriter_1 = jsonDf_1.write.json("file.json")
    val jsonWriter_2 = jsonDf_1.write.format("json").save("file.json")
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
