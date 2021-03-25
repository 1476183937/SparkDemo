package com.hnust.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Oper2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,4)

    val glomRDD: RDD[Array[Int]] = listRDD.glom()

    glomRDD.collect().foreach(item => {

      println("---------------")
      for (elem <- item) {
        println(elem)
      }
    })


  }


}
