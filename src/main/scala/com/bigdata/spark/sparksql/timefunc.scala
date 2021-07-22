package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object timefunc {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").config("spark.sql.session.timeZone", "IST").appName("timefunc").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val data = "C:\\bigdata\\datasets\\donations.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    //df.show(9)
    val res = df.withColumn("today", current_date()).withColumn("date", to_date($"date","dd-MM-yyyy")) //to_date used to convert string/timestamp to date format
    //  .orderBy($"date".desc)
      .withColumn("datediff", datediff($"today",$"date")) // number of days difference between two dates
      .withColumn("after2days", date_add($"date",-2)).withColumn("addmonth",add_months($"date",9))
      .withColumn("nextFriday",next_day($"today","Fri"))
      .withColumn("lastday", last_day($"date"))
      .withColumn("lastfri", next_day(date_add(last_day($"date"),-7),"Fri"))
      .withColumn("monween",floor(months_between($"today",$"date")))
    //  .withColumn("thirst", date_trunc("mon",$"today"))
     // .withColumn("qtr",quarter($"date"))
      .withColumn("dayofmonth", dayofmonth($"date"))
     // .withColumn("ts" , current_timestamp())
      .withColumn("dayofweek", dayofweek($"date"))
      .withColumn("dayofyr", dayofyear($"date"))
      .withColumn("ux",unix_timestamp())









    res.printSchema()
res.show(9)
    spark.stop()
  }
}