package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.importAll._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.util.Properties

object getmysqldata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SPARKPOC").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val qry = "(select * from emp where sal>2400) abc"
    val df  = spark.read.jdbc(murl,qry,mprop)
val cnt = df.count().toInt // how many number of records count it
   // val df = spark.read.format("jdbc").option("url",murl).option("user","mysqluser").option("password","mysqlpass").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","emp").load()
    df.show(cnt) //by default top 20 records ull get
      spark.stop()
  }
}