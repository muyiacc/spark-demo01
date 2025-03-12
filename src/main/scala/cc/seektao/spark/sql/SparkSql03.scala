package cc.seektao.spark.sql

object SparkSql03 {
  def main(args: Array[String]): Unit = {
    val props = System.getProperties
    println(props.get("jdk.module.illegalAccess")) // 应输出 "permit"
  }
}
