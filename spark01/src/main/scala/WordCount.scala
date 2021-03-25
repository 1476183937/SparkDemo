import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    //创建SarkConf并设置app名称
    val conf = new SparkConf().setAppName("WC")

    //创建SparkContext，该对象时spark App的入口
//    val sc = new SparkContext("local","sc",conf)
    val sc = new SparkContext(conf)

    //使用sc创建RDD并执行相应的transformation和action
    val tuples = sc.textFile("input").flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).collect()

    for (elem <- tuples) {
      println(elem)
    }

    //关闭连接
    sc.stop()

  }

}
