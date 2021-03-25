package com.hnust.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkSql_Demo {

  def main(args: Array[String]): Unit = {

    //SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")

    //创建SparkSql的环境对象：SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()



    //构建数据
    //    val userJson: DataFrame = spark.read.json("input/user.json")
    //展示数据
    //    userJson.show()

    import spark.implicits._

    //创建RDD
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 11), (2, "lisi", 12), (3, "ww", 13)))

    //转换为DF
    val rddDF: DataFrame = rdd.toDF("id", "name", "age")
    println("----------------- rddDF -----------------")
    rddDF.show()
    rddDF.createTempView("user")
    println("----------------- view -----------------")
    spark.sql("select * from user").show()

    //转换为DS
    val rddDS: Dataset[User] = rddDF.as[User]
    println("----------------- rddDS -----------------")
    rddDS.show()

    //转换为DF
    val dsToDF: DataFrame = rddDS.toDF()
    println("----------------- dsToDF -----------------")
    dsToDF.show()


    //转换为RDD
    val dfTodf: RDD[Row] = dsToDF.rdd
    println("----------------- dfTodf -----------------")
    dfTodf.collect().foreach(println)

    //rdd转换为DS
    val userRDD: RDD[User] = rdd.map{
      case (id, name, age) => {
        User(id, name, age)
      }
    }
    val userDS: Dataset[User] = userRDD.toDS()
    userDS.show()

    //释放资源
    spark.stop()

  }
  //样例类
  case class User(id:Int, name: String, age: Int)

}
