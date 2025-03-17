package cc.seektao.spark.core

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCore01 {
  def main(args: Array[String]): Unit = {

    // 创建运行配置
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("spark core test01")

    // 创建环境
    val sc = new SparkContext(sparkConf)

    // 统计单词出现的次数
    // wordCountWithSparkContext(sc)
//    wordCountWithSparkContextSimple(sc)

    // 连接数据库
    readFromMysql(sc)


    // 关闭 spark 连接
    sc.stop()
  }

  private def readFromMysql(sc: SparkContext) = {

  }

  private def wordCountWithSparkContextSimple(sc: SparkContext) = {
    // 读取文件
    val wordRDD = sc.textFile("data/input.txt")
    // 一键完成统计
    wordRDD.flatMap(word => word.split(" ")) // 分割
      .map(word => (word, 1)) // 转换数据结构
      .reduceByKey((x, y) => x + y)
      .collect()
      .foreach(println)

  }

  private def wordCountWithSparkContext(sc: SparkContext) = {
    // 读取文件
    val inputRDD = sc.textFile("data/input.txt")

    // 拆分单词
    val wordRDD = inputRDD.flatMap(word => word.split(" "))
//    wordRDD.collect().foreach(println)

    // 对单词转换结构
    val wordToMap = wordRDD.map(word => (word, 1))

    // 对单词分组
    val wordGroupByRDD = wordToMap.reduceByKey(_ + _)

    // 统计结果
    val resRDD = wordGroupByRDD.collect()

    // 打印结果
    resRDD.foreach(println)
  }

}
