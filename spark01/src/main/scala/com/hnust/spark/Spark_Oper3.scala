package com.hnust.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Oper3 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10,4)

    val groupByRDD: RDD[(Boolean, Iterable[Int])] = listRDD.groupBy(_%2==0)

    groupByRDD.collect().foreach(println)

  }


}
