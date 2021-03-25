package com.hnust.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Oper5 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)
    val distinct: RDD[Int] = sc.makeRDD(Array(1,1,2,2,3,4,4,5,5))

    //取样
//    val sampleRDD: RDD[Int] = listRDD.sample(false,0.4,1)

//    sampleRDD.collect().foreach(println)

    val distinctRDD: RDD[Int] = distinct.distinct()

    distinctRDD.collect().foreach(println)

  }


}
