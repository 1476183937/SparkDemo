package com.hnust.sparkSql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, LongType, StructType}

object SparkUDAF {

  def main(args: Array[String]): Unit = {

    //SparkConf
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSql")

    //创建SparkSql的环境对象：SparkSession
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val userRDD: DataFrame = spark.read.json("input/user.json")

    import spark.implicits._
    userRDD.createTempView("user")
    //创建自定义聚合函数
    val avg = new MyAvg
    //注册函数
    spark.udf.register("myavg", avg)
    //使用自定义函数
    spark.sql("select myavg(age) from user").show()


  }

  //求年龄的平均值
  class MyAvg extends UserDefinedAggregateFunction {
    //函数输入的数据结构
    override def inputSchema: StructType = {
      new StructType().add("age", LongType)
    }

    //缓冲区的数据结构
    override def bufferSchema: StructType = {
      new StructType().add("sum", LongType).add("count", LongType)
    }

    //返回结果的数据类型
    override def dataType: DataType = DoubleType

    //函数是否稳定
    override def deterministic: Boolean = true

    //buffer的初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      //将sum置为0
      buffer(0) = 0L
      //将count置为0
      buffer(1) = 0L
    }

    //根据查询结果更新缓冲区数据
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      //buffer(0)表示获取sum，buffer.getLong(0) 表示获取sum的值，
      // input.getLong(0)表示获取输入数据的第一个值，即age
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      //更新count+1
      buffer(1) = buffer.getLong(1) + 1
    }

    //合并多个节点的缓冲区
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      //buffer1(0)获取到的是sum，buffer1.getLong(0) + buffer2.getLong(0)表示获取两个缓冲区里的sum进行相加
      buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
      //合并count
      buffer1(0) = buffer1.getLong(1) + buffer2.getLong(1)
    }

    //计算
    def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble / buffer.getLong(1)
  }

}
