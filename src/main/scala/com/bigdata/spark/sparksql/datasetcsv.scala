package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object datasetcsv {
  case class uscc(first_name:String,last_name:String,company_name:String,address:String,city:String,county:String,state:String,zip:Int,phone1:String,phone2:String,email:String,web:String)
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("datasetcsv").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\us-500.csv"
val df=spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
  //  df.show(9)
    df.printSchema()
    val ds = df.as[uscc]
    ds.createOrReplaceTempView("tab")
    val res = spark.sql("select * from tab where state='NY'").as[uscc]
    res.show()
    spark.stop()
  }
}