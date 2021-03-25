package com.hnust.SparkStream

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountDStream {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("master").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(4))

    //设置检查点,需要在HDFS上创建文件streamCheck并修改权限为777
    ssc.checkpoint("hdfs://192.168.25.102:9000/streamCheck")
    //使用下面方法设置检查点是在本地目录上创建
//    ssc.sparkContext.setCheckpointDir("cp")

    //从指定主机指定端口获取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.25.102", 9999, StorageLevel.MEMORY_ONLY)

    //将数据扁平化，（word，1）
    val words: DStream[String] = lines.flatMap(_.split(" "))

    //对数据进行转换
    val wordsMap: DStream[(String, Int)] = words.map((_, 1))

    //updateStateByKey方法传入一个函数，values表示当前时间段获取到的数据，格式如：(a,1),(a,1)...
    //state表示之前时间段该key的数据,格式如：(a,3)
    //结果返回（a,5)
    val stateStream: DStream[(String, Int)] = wordsMap.updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      //对当前数据进行处理
      val currentCount: Int = values.foldLeft(0)(_ + _)
      //获取之前的数据
      val preCount: Int = state.getOrElse(0)
      //返回结果
      Some(currentCount + preCount)
    })
    //输出
    stateStream.print()

    //启动ssc
    ssc.start()
    ssc.awaitTermination()
  }

}
