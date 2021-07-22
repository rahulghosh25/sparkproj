package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.alldbconfig._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object oracledataimport {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oracledataimport").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val query = "(select * from EMP) b"
    val df = spark.read.jdbc(ourl,query,oprop)
    //another way to create dataframe
    //val df = spark.read.format("jdbc").option("url",ourl).option("user","orauser").option("password","oraclepass").option("driver","oracle.jdbc.OracleDriver").option("dbtable","EMP").load()
    df.show()
    spark.stop()
  }
}