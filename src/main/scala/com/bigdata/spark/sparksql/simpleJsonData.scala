package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object simpleJsonData {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("simpleJsonData").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
val data = "C:\\bigdata\\datasets\\2015-summary.json"
    val df = spark.read.format("json").load(data)
    df.show()
    //sql friendly
    spark.sql("create database testing")
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select DEST_COUNTRY_NAME, count(*) cnt from tab group by  DEST_COUNTRY_NAME order by cnt desc")
    res.printSchema()
   // res.show()
    spark.stop()
  }
}