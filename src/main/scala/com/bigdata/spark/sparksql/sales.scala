package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sales {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sales").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\sales.csv"
    val output = "C:\\bigdata\\datasets\\output\\sales"
    val df = spark.read.format("csv").option("header","true").option("infraSchema","true").load(data)
    df.createOrReplaceTempView("salestab")
    val res = spark.sql("select * from salestab")
    res.show()
    res.printSchema()
    res.write.mode("overwrite").format("csv").option("header","true").save(output)
    spark.stop()
  }
}