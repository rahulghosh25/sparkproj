package com.bigdata.spark.sparksql

import com.bigdata.spark.sparksql.alldbconfig._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mysqloracledataimport {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("mysqloracledataimport").getOrCreate()
    //    val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    import spark.implicits._
    import spark.sql
    val oradata = spark.read.jdbc(ourl,"dept",oprop)
    val sqldata = spark.read.jdbc(myurl,"emp",myprop)
    //alternet way
    //val oradata = spark.read.format("jdbc").option("url",ourl).option("user","orauser").option("password","oraclepass").option("driver","oracle.jdbc.OracleDriver").option("dbtable","dept").load()
    //val sqldata = spark.read.format("jdbc").option("url",myurl).option("user","mysqluser").option("password","mysqlpass").option("driver","com.mysql.cj.jdbc.Driver").option("dbtable","emp").load()
    oradata.createOrReplaceTempView("dept")
    sqldata.createOrReplaceTempView("emp")
    val joindata = spark.sql("select emp.*, dept.dname,dept.loc from emp join dept on emp.deptno=dept.deptno")
    joindata.show()
    joindata.write.mode(SaveMode.Overwrite).jdbc(msurl,"joindata",msprop)
    spark.stop()
  }
}