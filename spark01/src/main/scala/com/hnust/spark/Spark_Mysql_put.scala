package com.hnust.spark

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Mysql_put {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("HBaseApp")
    val sc = new SparkContext(sparkConf)

    //创建rdd
    val datas = sc.parallelize(List("Female1", "Male1","Female1"))

    //遍历rdd，将数据插入数据库。必须将数据库的连接操作也放入遍历过程中，放到外面显示位序列化，
    //为了避免过多的创建connection，使用foreachPartition方法而不使用foreach方法，一个分区创建一个连接
    datas.foreachPartition(data =>{
      Class.forName ("com.mysql.jdbc.Driver").newInstance()
      val connection: Connection = DriverManager.getConnection("jdbc:mysql://192.168.25.102:3306/rdd","root","root")

      val sql:String = "insert into rddTable (name) values(?)"

      val prest: PreparedStatement = connection.prepareStatement(sql)

//      prest.setString(1,data)
      prest.executeUpdate()

      connection.close()
      prest.close()
    })





//    data.foreachPartition(insertData)
  }

  /*def insertData(iterator: Iterator[String]): Unit = {
    Class.forName ("com.mysql.jdbc.Driver").newInstance()
    val conn = java.sql.DriverManager.getConnection("jdbc:mysql://192.168.25.102:3306/rdd", "root", "root")
    iterator.foreach(data => {
      val ps = conn.prepareStatement("insert into rddTable(name) values (?)")
      ps.setString(1, data)
      ps.executeUpdate()
    })
  }*/


}

