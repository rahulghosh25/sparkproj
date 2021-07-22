package com.bigdata.spark.sparksql

import org.apache.spark.sql._

object testing {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("testing").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val list = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

    val d = sc.parallelize(list)
    // #collection of objects
    val d2 = d.map(x => x * x)
    val coll = d2.collect.foreach(print)
    //    print(coll)
    spark.stop()
  }
}