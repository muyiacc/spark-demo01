package cc.seektao.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import java.util.Properties

object SparkSqlReadHive {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("spark sql read hive")
      .setMaster("local[*]")
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // code
    readHive(spark)

    spark.close()
  }

  private def readHive(spark: SparkSession) = {
    spark.sql("show tables").show()
    spark.sql("select * from student").show()
  }
}
