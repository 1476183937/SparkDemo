package com.hnust.spark

import java.util

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

object Spark_AccumulatorV2 {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("WC").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)

    val wordsRDD: RDD[String] = sc.makeRDD(List("Hadoop","hhh","jjj","hjhj","lll"))

    //创建累加器
    val wordAccumulator = new WordAccumulator

    //注册累加器
    sc.register(wordAccumulator)
    val list = List((1,1),(2,2),(3,3))
    val broadcast: Broadcast[List[(Int, Int)]] = sc.broadcast(list)
    broadcast.value

    val accumulatorRes: Unit = wordsRDD.foreach {
      case word => {
        //执行累加器的功能
        wordAccumulator.add(word)
      }
    }

    println("结果： "+wordAccumulator.value)
  }

  /**
    * String表示输出的类型
    * java.util.ArrayList[String]表示返回的类型
    */
  class WordAccumulator extends AccumulatorV2[String,java.util.ArrayList[String]]{

    //定义一个数组，用于返回
    private val list = new util.ArrayList[String]()

    //判断当前累加器是否为初始化状态
    override def isZero: Boolean = list.isEmpty

    //复制累加器对象
    override def copy(): AccumulatorV2[String, util.ArrayList[String]] = new WordAccumulator()

    //重置累加器
    override def reset(): Unit = list.clear()

    //向累加器中添加数据
    override def add(v: String): Unit = {
      //如果数据包含“h”，就加入累加器
      if (v.contains("h")){
        list.add(v)
      }
    }

    //合并累加器
    override def merge(other: AccumulatorV2[String, util.ArrayList[String]]): Unit = {
      //合并其他累加器的结果
      list.addAll(other.value)
    }

    //获取累加器的结果
    override def value: util.ArrayList[String] = list
  }


}
