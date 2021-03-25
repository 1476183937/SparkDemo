package com.hnust.SparkStream

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object TextFileStream {

  def main(args: Array[String]): Unit = {

    /*val sparkConf: SparkConf = new SparkConf().setAppName("master").setMaster("local[*]")

    //初始化StreamingContext，设置为将4s内的数据作为一个RDD
    val streamingContext = new StreamingContext(sparkConf,Seconds(4))

    //创建duilie
//    val rddQueue = new mutable.Queue[RDD[Int]]()
//
//    //创建queueStream，传入rddQueue，并将是否只可被消费一次
//    val queueStream: InputDStream[Int] = streamingContext.queueStream(rddQueue,oneAtATime = false)

    //3.创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //4.创建QueueInputDStream
    val queueStream = streamingContext.queueStream(rddQueue,oneAtATime = false)

    //对单词进行转换=>(word,1)
    val wordTuples: DStream[(Int, Int)] = queueStream.map((_,1))

    //对单词进行统计
    val wordCountResult: DStream[(Int, Int)] = wordTuples.reduceByKey(_+_)

    wordCountResult.print()

    //启动streamingContext,
    streamingContext.start()

    //往队列里添加数据
   for (i <- 1 to 5){
     rddQueue += streamingContext.sparkContext.makeRDD(1 to 100,10)
     Thread.sleep(3000)
   }

    streamingContext.awaitTermination()*/

    //1.初始化Spark配置信息
    val conf = new SparkConf().setMaster("local[*]").setAppName("RDDStream")
    //2.初始化SparkStreamingContext
    val ssc = new StreamingContext(conf, Seconds(4))
    //3.创建RDD队列
    val rddQueue = new mutable.Queue[RDD[Int]]()
    //4.创建QueueInputDStream
    val inputStream = ssc.queueStream(rddQueue,oneAtATime = false)
    //5.处理队列中的RDD数据
    val mappedStream = inputStream.map((_,1))
    val reducedStream = mappedStream.reduceByKey(_ + _)
    //6.打印结果
    reducedStream.print()
    //7.启动任务
    ssc.start()
    //8.循环创建并向RDD队列中放入RDD
    for (i <- 1 to 5) {
      rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
      Thread.sleep(2000)
    }

    ssc.awaitTermination()

  }

}
