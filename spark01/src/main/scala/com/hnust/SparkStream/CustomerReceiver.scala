package com.hnust.SparkStream

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CustomerReceiver {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("master")

    val streamingContext = new StreamingContext(sparkConf, Seconds(4))

    //使用自定义接收器
    val lineStreams: ReceiverInputDStream[String] = streamingContext.receiverStream(new CustomerReceiver("192.168.25.102",9999))

    //将RDD里的每一行数据进行切分扁平化,得到一个一个的单词
    val words: DStream[String] = lineStreams.flatMap(_.split(" "))

    //对单词进行转换=>(word,1)
    val wordTuples: DStream[(String, Int)] = words.map((_,1))

    //对单词进行统计
    val wordCountResult: DStream[(String, Int)] = wordTuples.reduceByKey(_+_)

    wordCountResult.print()

    //启动streamingContext
    streamingContext.start()
    streamingContext.awaitTermination()


  }

  class CustomerReceiver(hostname: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    //在Receiver启动时会调用，作用：给spark发送数据
    override def onStart(): Unit = {
      //启动一个线程，发送数据
      new Thread(new Runnable {

        override def run(): Unit = {
          receive()
        }
      }).start()

    }

    override def onStop(): Unit = {

    }

    //读数据并将数据发送给Spark
    def receive(): Unit = {

      //创建一个socket，读取数据
      val socket = new Socket("192.168.25.102", 9999)

      //定义一个变量来接收数据
      var line: String = null;

      //获取输入流
      val reader = new BufferedReader(new InputStreamReader(socket.getInputStream,"UTF-8"))

      //读取数据
      line = reader.readLine()

      //当receiver没有关闭并且输入数据不为空，则循环发送数据给Spark
      while (!isStopped() && line != null){
        //发送数据
        store(line)
        //读取下一行
        line = reader.readLine()
      }


      //关闭资源
      reader.close()
      socket.close()

      restart("restart")
    }
  }

}
