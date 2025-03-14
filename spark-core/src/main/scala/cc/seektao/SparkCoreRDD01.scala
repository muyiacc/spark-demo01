package cc.seektao.spark.core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkCoreRDD01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[*]")
      .setAppName("create rdd")

    val sc = new SparkContext(sparkConf)

    // create rdd
//    createRDD(sc)

    // RDD 并行度与分区
//    rddNumSlices(sc)

    // RDD 转换算子
    // 单 value
    rddConversionOperatorSingleValue(sc)

    sc.stop()
  }

  private def rddConversionOperatorSingleValue(sc: SparkContext) = {
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9), 3)

    /**
     * map
     * 将处理的数据逐条进行映射转换，这里的转换可以是类型的转换，也可以是值的转换。
     */
    println("==========map===========")
    rdd.map(value => value + 1).foreach(println) // 打印所有元素
    println("============")
    rdd.map(value => value + "").foreach(println)

    /**
     * mapPartitions
     * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
     * 理，哪怕是过滤数据
     */
    println("=========mapPartitions============")
    rdd.mapPartitions(values =>
      values.filter(value => value > 2) // 过滤：找出大于2
    ).foreach(println)
    println("获取每个分区的最大值")
    val result = rdd.mapPartitions(partition =>
      if (partition.isEmpty) {
        Iterator.empty
      } else {
        Iterator(partition.max)
      }
    ).collect()
    println("每个分区最大值：", result.mkString(", "))

    /**
     * mapPartitionsWithIndex
     * 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处
     * 理，哪怕是过滤数据，在处理时同时可以获取当前分区索引。
     */
    println("=======mapPartitionsWithIndex=========")
    rdd.mapPartitionsWithIndex((index, values) => {
      values.map(value => (index, value))
    }).foreach(println)
    println("获取第二个数据分区的数据")
    rdd.mapPartitionsWithIndex((index, values) => {
      if (index == 2 ){
        values.map(value => (index, value))
      } else {
        Iterator.empty
      }
    }).foreach(println)

    /**
     * flatMap
     * 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
     */
      println("==========flatMap===========")
    val flatMapRDD = sc.makeRDD(List(
      List(1,2),List(3,4)
    ))
    flatMapRDD.flatMap(list => list)
      .foreach(println)
    println("将 List(List(1,2),3,List(4,5))进行扁平化操作")
    val flatMapRDD2 = sc.makeRDD(List(
      List(1,2)
      ,3
      ,List(3,5)
    ))
    flatMapRDD2.flatMap {
      case item: List[_] => item
      case item => List(item)
    }.foreach(println)


    /**
     * glom
     * 将同一个分区的数据直接转换为相同类型的内存数组进行处理，分区不变
     */
    val glomRDD = sc.makeRDD(
      List(1,2,3,4)
      ,1
    )
    val glomRDD2: RDD[Array[Int]] = glomRDD.glom()
    println("计算所有分区最大值求和（分区内取最大值，分区间最大值求和）")
    val glomRDD3 = sc.makeRDD(
      List(1,2,3,4,5,6,7,8,9)
      ,3
    )
    val maxValueRDD = glomRDD3.glom().map {
      partitionArray =>
        if (partitionArray.isEmpty) 0
        else partitionArray.max
    }
    println("每个分区最大值：", maxValueRDD.collect().mkString(", "))
    println("每个分区最大值的和：", maxValueRDD.sum())




  }

  private def rddNumSlices(sc: SparkContext) = {
    val rdd = sc.parallelize(
      List(1,2,3)
      ,2 // 并行度
    )
    rdd.collect().foreach(println)

    val rdd2 = sc.makeRDD(
      List(4,5,6)
      ,2 // 并行度
    )
    rdd2.foreach(println)
  }

  private def createRDD(sc: SparkContext) = {
    // method 1
    val rdd1 = sc.parallelize(
      List(1,2,3)
    )
    // method 2
    val rdd2 = sc.makeRDD(
      List(4,5,6)
    )
    rdd1.collect().foreach(println)
    rdd2.collect().foreach(println)

    // method 3
    val rdd3 = sc.textFile("data/input.txt")
    rdd3.collect().foreach(println)
  }

}
