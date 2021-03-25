package com.hnust.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_RDD {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    //1.从内存中创建RDD方式一： makeRDD
    val listRDD: RDD[Int] = sc.makeRDD(List(1,2,3,4))

    //使用自定义分区
    val listRDD1: RDD[Int] = sc.makeRDD(List(1,2,3,4),2)

    //2.从内存创建RDD方式二
    val parallelizeRDD: RDD[Int] = sc.parallelize(List(1,2,3,4))

    //3.从外部存储中创建
    //默认情况下，读取项目路径，也可以读取其他路径
    //默认从文件中读取的数据都是字符串
    //读取文件时，传递的分区数为最小的分区数，但不一定是这个分区，取决于hadoop读取文件的分片规则
//    val textFileRDD: RDD[String] = sc.textFile("input")
    val textFileRDD: RDD[String] = sc.textFile("input",2)

    //将RDD的数据存储到文件
    parallelizeRDD.saveAsTextFile("output")

  }

}
