package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.alldbconfig._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mysqldataimport {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mysqldataimport").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val query = "(select * from emp)a"
    val df = spark.read.jdbc(myurl,query,myprop)
    //another way to create dataframe
    //val df = spark.read.format("jdbc").option("url",myurl).option("user","mysqluser").option("password","mysqlpass").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","emp").load()
    df.show()
    spark.stop()
  }
}