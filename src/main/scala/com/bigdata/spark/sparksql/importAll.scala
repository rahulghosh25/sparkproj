package com.bigdata.spark.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.util.Properties

object importAll {

  val murl = "jdbc:mysql://mysqldb.cto8cz9itkcp.ap-south-1.rds.amazonaws.com:3306/mydb"
  val mprop = new Properties()
  mprop.setProperty("user","mysqluser")
  mprop.setProperty("password","mysqlpass")
  mprop.setProperty("driver","com.mysql.cj.jdbc.Driver")

  val ourl = "jdbc:oracle:thin:@//myoracledb.cto8cz9itkcp.ap-south-1.rds.amazonaws.com:1521/ORCL"
  val oprop = new Properties()
  oprop.setProperty("user","orauser")
  oprop.setProperty("password","oraclepass")
  oprop.setProperty("driver","oracle.jdbc.OracleDriver")

  val msurl = "jdbc:sqlserver://mssql.cto8cz9itkcp.ap-south-1.rds.amazonaws.com:1433;databaseName=rahuldb;"
  val msprop = new Properties()
  msprop.setProperty("user","msusername")
  msprop.setProperty("password","mspassword")
  msprop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")

}