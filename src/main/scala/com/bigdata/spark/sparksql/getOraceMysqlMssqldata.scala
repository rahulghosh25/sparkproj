package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import com.bigdata.spark.sparksql.importAll._
object getOraceMysqlMssqldata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("getOraceMysqlMssqldata").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql

    val mdf = spark.read.jdbc(murl,"emp",mprop)
    val odf = spark.read.jdbc(ourl,"dept",oprop)
    mdf.createOrReplaceTempView("e")
    odf.createOrReplaceTempView("d")
    val join = spark.sql("select e.*, d.dname,d.loc from e join d on e.deptno=d.deptno")
    join.show()
    join.write.mode(SaveMode.Overwrite).jdbc(msurl,"empdeptjoin",msprop)

    spark.stop()
  }
}