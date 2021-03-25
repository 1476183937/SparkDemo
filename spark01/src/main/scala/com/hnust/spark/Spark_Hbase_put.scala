package com.hnust.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Hbase_put {

  def main(args: Array[String]): Unit = {


    //1.创建spark配置信息
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("JdbcRDD")

    //2.创建SparkContext
    val sc = new SparkContext(sparkConf)

    //获取Hbase的配置
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104")

    //创建jobConf对象，
    val jobConf = new JobConf(conf)
    //设置输出格式
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    //设置输出到哪张表
    jobConf.set(TableOutputFormat.OUTPUT_TABLE,"student")

    //构建数据RDD
    val dataRDD: RDD[(String, String)] = sc.makeRDD(List(("1004","zhangsan"),("1002","lisi"),("1003","wanwu")))

    //将RDD数据转换成（new ImmutableBytesWritable(rowKey),Put）元组
    val putRDD: RDD[(ImmutableBytesWritable, Put)] = dataRDD.map {
      case (rowKey, value) => {
        //创建put对象
        val put: Put = new Put(Bytes.toBytes(rowKey))
        //添加列族和列值
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(value))

        //返回值
        (new ImmutableBytesWritable(Bytes.toBytes(rowKey)), put)
      }
    }

    //执行保存操作
    putRDD.saveAsHadoopDataset(jobConf)

    //关闭资源
    sc.stop()


  }


}

