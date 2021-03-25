package com.hnust.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Oper1 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val listRDD: RDD[Int] = sc.makeRDD(1 to 10)

    //mapPartitions可以对一个RDD中的所有分区进行遍历，是以分区为单位传递数据到Executor的，
    //而map是以分区里面的每一条数据为传递单位
    //mapPartitions效率优于map算子，减少了发送数据到执行器的次数
    //mapPartitions可能会内存溢出
    //    val mapRDD: RDD[Int] = listRDD.map(_*2)
    val mapPartitionsRDD: RDD[Int] = listRDD.mapPartitions(datas => {

      datas.map(_ * 2)
    })


    //mapPartitionsWithIndexRDD:每一条数据都会有分区，下面的num即为该数据所在分区
        val mapPartitionsWithIndexRDD: RDD[(Int, String)] = listRDD.mapPartitionsWithIndex {
          case (num, datas) => {
            datas.map((_, "分区是：" + num))
          }
        }



    mapPartitionsWithIndexRDD.foreach(println)

  }


}
