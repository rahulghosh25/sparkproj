package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.alldbconfig._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object oracle2mysqlmultipletable {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oracle2mysqlmultipletable").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val tables = List("BONUS","CUSTOMER","DUMMY")
    tables.foreach{x=> val df = spark.read.jdbc(ourl,s"$x",oprop)
    df.show()
    df.write.mode(SaveMode.Append).jdbc(myurl,s"$x",myprop)
    }
    spark.stop()
  }
}