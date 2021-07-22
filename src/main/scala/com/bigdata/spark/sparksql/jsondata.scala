package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object jsondata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("jsondata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\zips.json"
    val df = spark.read.format("json").load(data)
   // df.show(4)
   // df.printSchema()
df.createOrReplaceTempView("tab")
    //sql friendly
    //val res = spark.sql("select _id id, city, loc[0] lang, loc[1] lati, pop, state from tab")
    //scala/python friendly
    val res = df.withColumnRenamed("_id","id")
      .withColumn("lang", col("loc")(0)) // $ represent column/refer something.
      .withColumn("lati",$"loc"(1))
      .drop($"loc")
    res.show(8)
    res.printSchema()
    spark.stop()
  }
}