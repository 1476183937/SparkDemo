package com.hnust.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Oper4 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[String] = sc.makeRDD(Array("hello","kkkk","ffff","yyyy","hjjj"))

    //包含“h“的留下
    val filterRDD: RDD[String] = listRDD.filter(_.contains("h"))

    filterRDD.collect().foreach(println)

  }


}
