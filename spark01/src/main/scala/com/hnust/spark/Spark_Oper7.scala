package com.hnust.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object Spark_Oper7 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val data: RDD[(Int, Int)] = sc.parallelize(Array((11,1),(21,2),(31,3),(42,4),(52,5),(63,6)))

    val par: RDD[(Int, Int)] = data.partitionBy(new MyPartition(4))

    val partitioned: RDD[(Int, (Int, Int))] = par.mapPartitionsWithIndex((index, datas) => {
      datas.map((index, _))
    })
    partitioned.collect().foreach(println)



  }


  //将具有相同结尾的放入同一个分区
  class MyPartition(partitions: Int)  extends Partitioner{
    override def numPartitions: Int = partitions

    override def getPartition(key: Any): Int = {
      val ckey: String = key.toString

      ckey.substring(ckey.length-1).toInt%partitions

    }
  }
}

