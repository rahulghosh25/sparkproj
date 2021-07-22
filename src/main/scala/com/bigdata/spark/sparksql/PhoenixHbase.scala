package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// com.bigdata.spark.sparksql.PhoenixHbase
object PhoenixHbase {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("PhoenixHbase").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val tab = "usdata"
//val df=spark.read.format("org.apache.phoenix.spark").option("table",tab).option("zkUrl","localhost:2181").load()
  //  df.show()
    val data = args(0)
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.write.mode(SaveMode.Overwrite).format("org.apache.phoenix.spark").option("table",tab).option("zkUrl","localhost:2181").save()
df.show()

    //  .options(Map("table" -> "TABLE1", PhoenixDataSource.ZOOKEEPER_URL -> "phoenix-server:2181"))
 //
  }
}