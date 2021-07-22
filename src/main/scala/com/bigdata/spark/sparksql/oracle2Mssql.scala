package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.importAll._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object oracle2Mssql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oracle2Mssql").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
val all = "(select TABLE_NAME from all_tables where tablespace_name='USERS') q"
    // list all tables from oracle
    val tabs = spark.read.jdbc(ourl,all,oprop).rdd.map(x=>x(0)).collect.toList
    // list of tables get it in list/array
  //  val tabs = List("EMP","DEPT","ITEM") //multiple table
    tabs.foreach{x=>
      val df =spark.read.jdbc(ourl, s"$x",oprop)
      df.show()
      df.write.mode(SaveMode.Append).jdbc(msurl, s"$x",msprop)
    }




    spark.stop()
  }
}