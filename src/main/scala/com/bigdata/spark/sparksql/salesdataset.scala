package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.functions._

object salesdataset {
  case class sales(InvoiceNo:String, StockCode:String, Description:String, Quantity:Int, InvoiceDate:Timestamp, UnitPrice:Double, CustomerID:Double, Country:String)
  case class quantity(InvoiceNo:String,Totalquantity:BigInt)
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("salesdataset").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext



    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\2010-12-01.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    val ds = df.as[sales]
    ds.createOrReplaceTempView("sales")
    val res = spark.sql("select InvoiceNo,sum(Quantity) Totalquantity from sales group by InvoiceNo").as[quantity]
    res.show()
    res.printSchema()
    spark.stop()
  }
}