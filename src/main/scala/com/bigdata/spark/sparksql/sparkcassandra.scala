package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object sparkcassandra {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("sparkcassandra").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val df = spark.read.format("org.apache.spark.sql.cassandra").option("testdb", "manishks").option("table", "asl").load()
    val df1 = spark.read.format("org.apache.spark.sql.cassandra").option("testdb", "manishks").option("table", "emp").load()
    df.show()
    df1.show()
    df.createOrReplaceTempView("a")
    df1.createOrReplaceTempView("e")
    val join =  spark.sql("select a., e. from a join e on a.name=e.first_name").drop("first_name")
    join.show()

    join.write.mode(SaveMode.Append).format("org.apache.spark.sql.cassandra").option("keyspace", "testdb").option("table", "jointab").save()
    spark.stop()
  }
}