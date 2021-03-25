package com.hnust.SparkStream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WordCountStream {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setAppName("master").setMaster("local[*]")

    //初始化StreamingContext，设置为将4s内的数据作为一个RDD
    val streamingContext = new StreamingContext(sparkConf,Seconds(4))

    //通过监控端口9999发进来的数据，将4秒内的数据放入一个RDD，数据是一行一行的
    val lineStreams: ReceiverInputDStream[String] = streamingContext.socketTextStream("192.168.25.102",9999)

    //将RDD里的每一行数据进行切分扁平化,得到一个一个的单词
    val words: DStream[String] = lineStreams.flatMap(_.split(" "))

    //对单词进行转换=>(word,1)
    val wordTuples: DStream[(String, Int)] = words.map((_,1))

    //对单词进行统计
    val wordCountResult: DStream[(String, Int)] = wordTuples.reduceByKey(_+_)

    wordCountResult.print()

    //启动streamingContext,
    streamingContext.start()
    streamingContext.awaitTermination()




  }

}
