package com.hnust.spark

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Mysql_get {

  def main(args: Array[String]): Unit = {

    /*val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.25.102:3306/rdd"
    val user = "root"
    val paswd = "root"

    val jdbcRDD = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, user, paswd)
    },
      "select * from rddTable where id>?",
      1,
      3,
      1,
      r => (r.getObject(1), r.getObject(2))
    )
    println(jdbcRDD.count())
    jdbcRDD.foreach(println)

    sc.stop()*/

    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //3.定义连接mysql的参数
    val driver = "com.mysql.jdbc.Driver"
    val url = "jdbc:mysql://192.168.25.102:3306/rdd"
    val userName = "root"
    val passWd = "root"

    //创建JdbcRDD
    val rdd = new JdbcRDD(sc, () => {
      Class.forName(driver)
      DriverManager.getConnection(url, userName, passWd)
    },
      "select * from `rddTable` where id >= ? and id <=?",
      1,
      10,
      1,
      r => {
        (r.getObject(1), r.getObject(2))
      }
    )

    //打印最后结果
//    println(rdd.count())
    rdd.collect().foreach(println)
//    rdd.saveAsTextFile("output")

    sc.stop()


  }


}

