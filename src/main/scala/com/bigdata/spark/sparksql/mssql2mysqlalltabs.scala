package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.alldbconfig._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mssql2mysqlalltabs {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mssql2mysqlalltabs").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val tables = "(SELECT name FROM SYSOBJECTS WHERE xtype = 'U') a"
    print(tables)
    val tabs = spark.read.jdbc(msurl,tables,msprop).rdd.map(x=>x(0)).collect.toList
    print(tabs)
    tabs.foreach{x=> val df = spark.read.jdbc(msurl,s"$x",msprop)
    df.show()
    df.write.mode(SaveMode.Append).jdbc(myurl,s"$x",myprop)
    }
    spark.stop()
  }
}