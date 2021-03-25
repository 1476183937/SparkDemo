package com.hnust.SparkStream

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountWindow {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("master").setMaster("local[*]")

    val ssc = new StreamingContext(sparkConf, Seconds(3))

    //设置检查点,需要在HDFS上创建文件streamCheck并修改权限为777
    ssc.checkpoint("hdfs://192.168.25.102:9000/streamCheck")
    //使用下面方法设置检查点是在本地目录上创建
//    ssc.sparkContext.setCheckpointDir("cp")

    //从指定主机指定端口获取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.25.102", 9999, StorageLevel.MEMORY_ONLY)

    //设置窗口，窗口大小为9s，步长为3s，这两个参数大小必须为上面ssc里面设置的时间的整数倍
    val windowLines: DStream[String] = lines.window(Seconds(9),Seconds(3))

    //将数据扁平化，（word，1）
    val words: DStream[String] = windowLines.flatMap(_.split(" "))

    //对数据进行转换
    val wordsMap: DStream[(String, Int)] = words.map((_, 1))

    //处理数据
    val stateStream: DStream[(String, Int)] = wordsMap.reduceByKey(_+_)
    //输出
    stateStream.print()

    //启动ssc
    ssc.start()
    ssc.awaitTermination()
  }

}
