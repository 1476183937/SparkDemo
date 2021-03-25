package com.hnust.sparkSql

import java.util.Properties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSql_get {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","root")

    //从数据库读取数据
//    val dataFrame: DataFrame = spark.read.jdbc("jdbc:mysql://192.168.25.102:3306/rdd","rddTable",properties)
    val dataFrame: DataFrame = spark.read.format("jdbc").option("user", "root").option("password", "root").option("url", "jdbc:mysql://192.168.25.102:3306/rdd")
      .option("dbtable", "rddTable").load()


    dataFrame.show()

    //向数据库写入数据
//    dataFrame.write.jdbc("jdbc:mysql://192.168.25.102:3306/rdd","rddTable1",properties)
    dataFrame.write.format("jdbc").option("user", "root").option("password", "root").option("url", "jdbc:mysql://192.168.25.102:3306/rdd")
      .option("dbtable", "rddTable2").save()
  }

}
