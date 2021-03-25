package com.hnust.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Oper6 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 16, 4)

    println("前分区=" + listRDD.partitions.size)
    val coalesceRDD: RDD[Int] = listRDD.coalesce(3)

    println("后分区=" + coalesceRDD.partitions.size)
    coalesceRDD.saveAsTextFile("output")
    //    coalesceRDD.collect().foreach(println)

  }


}
