package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object csvdataset {
  case class abcd(transactionId:Int, customerId:Int, itemID:Int, amountPaid:Double)
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("csvdataset").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\sales.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.printSchema()
val ds = df.as[abcd]
    ds.show()
    spark.stop()
  }
}