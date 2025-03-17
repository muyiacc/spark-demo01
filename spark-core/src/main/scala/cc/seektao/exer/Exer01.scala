package cc.seektao.spark.core.exer

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.annotation.switch

object Exer01 {
  def main(args: Array[String]): Unit = {
    // 创建环境
    val sparkConf = new SparkConf()
      .setAppName("spark rdd exer 01")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    // code
    exerMap(sc)
    exerMapPartitions(sc)
    exerMapPartitionsWithIndex(sc)
    exerFlatMap(sc)
    exerGlom(sc)

    // 关闭环境
    sc.stop()
  }

  private def exerGlom(sc: SparkContext) = {
    println("==========exerGlom==============")
    val rdd = sc.makeRDD(List(1,2,3,4,5,6,7,8,9), 3)
    println("计算所有分区最大值求和（分区内取最大值，分区间最大值求和）")
    
  }

  private def exerFlatMap(sc: SparkContext) = {
    println("============exerFlatMap==============")
    val rdd = sc.makeRDD(List(
        List(1,2,3),4,5,6,List(7,8,9)
    ))
    println("将List(1,2,3),4,5,6,List(7,8,9)进行扁平化操作")
    val resRDD = rdd.flatMap(data => data match {
        case data: List[_] => data
        case _ => List(data)
    })
    resRDD.foreach(println)
  }

  private def exerMapPartitionsWithIndex(sc: SparkContext) = {
    println("==========mapPartitionsWithIndex===========")
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6, 7, 8, 9),
      3
    )
    println("获取第三个数据分区为偶数的数据")
    val resRDD = rdd.mapPartitionsWithIndex((index, values) =>
      if (index == 2) values.filter(value => value % 2 == 0)
      else Iterator.empty
    )
    resRDD.foreach(println)
  }

  private def exerMapPartitions(sc: SparkContext) = {
    println("=========exerMapPartitions==========")
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6, 7, 8, 9),
      3
    )
    println("获取每个数据分区的最小值")
    val resRDD = rdd.mapPartitions(values => {
      Iterator(values.min)
    })
    resRDD.foreach(println)
  }

  private def exerMap(sc: SparkContext) = {
    println("=======map===========")
    val rdd = sc.makeRDD(
      List(1, 2, 3, 4, 5, 6, 7, 8)
    )
    println("对奇数加2，对偶数加10")
    val resRDD = rdd.map(value =>
      value match {
        case _ if value % 2 == 1 => value + 2
        case _ if value % 2 == 0 => value + 10
      }
    )
    resRDD.foreach(println)
  }
}
