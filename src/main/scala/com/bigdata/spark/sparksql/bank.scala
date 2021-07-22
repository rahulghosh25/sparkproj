package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// com.bigdata.spark.sparksql.bank
object bank {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("bank").enableHiveSupport().getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
/*val data = "C:\\bigdata\\datasets\\bank-full.csv"
    val output =  "C:\\bigdata\\datasets\\output\\bank-full"*/
    val data = args(0)
    //val output = args(1)
    val table = args(1)
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").option("sep",";").load(data)
  //  df.printSchema() //display columns, datatype in nice tree format
  //  df.show(5) // show display top 20 records by default if u mention 5 only top 5 records u ll get
    //processing
    df.createOrReplaceTempView("tab") // run sql queries on top of dataframe
    val res = spark.sql("select * from tab where age>70 and balance>80000 ")
    res.show()
    //res.coalesce(1).write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(output)
    res.write.mode("overwrite").format("hive").option("path","/sparkhive/externalpath").saveAsTable(table)


    spark.stop()
  }
}