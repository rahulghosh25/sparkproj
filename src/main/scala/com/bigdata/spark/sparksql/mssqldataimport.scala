package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.alldbconfig._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mssqldataimport {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mssqldataimport").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val query = "(select * from EMP) c"
    val df = spark.read.jdbc(msurl,query,msprop)
    //another way to create dataframe
    //val df = spark.read.format("jdbc").option("url",msurl).option("user","msusername").option("password","mspassword").option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver").option("dbtable","emp").load()
    df.show()
    spark.stop()
  }
}