package cc.seektao.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

object SparkSql02 {
  case class Person(name: String, age: Int)
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("spark sql")
      // .setMaster("local[*]")
      .setMaster("local-cluster[8,1,512]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    // code
    // Inferring the Schema Using Reflection
    runBaseExample(spark)

    spark.close()
  }

  private def runBaseExample(spark: SparkSession) = {
    import spark.implicits._
    // Create an RDD of Person objects from a text file, convert it to a Dataframe
    val file = "src/main/resources/people.txt"
    val peopleDF = spark.sparkContext
      .textFile(file)
      .map(_.split(","))
      .map(item => Person(item(0), item(1).trim.toInt))
      .toDF()

    // Register the DataFrame as a temporary view
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by Spark
    val teenagersDF =
      spark.sql("select * from people where age between 13 and 19")

    // The columns of a row in the result can be accessed by field index
    teenagersDF.map(teenager => "Name: " + teenager(0)).show()

    // or by field name
    teenagersDF
      .map(teenager => "Name: " + teenager.getAs[String]("name"))
      .show()

    // No pre-defined encoders for Dataset[Map[K,V]], define explicitly
    // implicit val mapEncoder =
      org.apache.spark.sql.Encoders.kryo[Map[String, Any]]

    // row.getValuesMap[T] retrieves multiple columns at once into a Map[String, T]
    // teenagersDF
    //   .map(teenager => teenager.getValuesMap[Any](List("name", "age")))
    //   .collect()
  }
}
