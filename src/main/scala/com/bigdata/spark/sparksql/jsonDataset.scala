package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object jsonDataset {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsonDataset").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
val data = "C:\\bigdata\\datasets\\zips.json"

val df = spark.read.format("json").load(data)
    df.printSchema()

    val ds = df.as[zipcc]
    ds.createOrReplaceTempView("tab")
    val res = spark.sql("select _id id, city, loc[0] lang, loc[1] lati,pop, state from tab").as[finalcc]

    res.show()

    spark.stop()
  }
  case class finalcc(id:String, city:String, lang:Double,lati:Double,pop:Long, state:String)
  case class zipcc (_id:String, city:String,loc:Array[Double],pop:Long,state:String)

}