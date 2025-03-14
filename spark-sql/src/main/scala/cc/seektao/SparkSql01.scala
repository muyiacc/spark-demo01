package cc.seektao.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.arrow.flatbuf.SparseTensor
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DataTypes
import javax.xml.crypto.Data
import java.util.Hashtable

object SparkSql01 {
  case class Person(name: String, age: Long)
  case class User(age: Long, name: String)

  def main(args: Array[String]): Unit = {

    // 创建 spark conf
    val conf: SparkConf =
      new SparkConf().setMaster("local[*]").setAppName("Test01")

    // 创建 spark session
    val spark = SparkSession.builder().config(conf).getOrCreate()

    // 指定日志级别
    // spark.sparkContext.setLogLevel("warn")

    // runBasicDataFrameExample(spark)
    // runBasicDateSetExample(spark)

    // 读取 json 文件，并展示内容
    // readJson(spark)
    // readJsonToWrite(spark)
    // readJsonSpecifySchema(spark)
    // readJsonSpecifySchema2(spark)

    readJDBC(spark)

    // 关闭 spark session
    spark.close()
  }

  def readJDBC(spark: SparkSession) = {
    val url = "jdbc:mysql://localhost:3306/atguigu"
    val table = "employees"
    val properties = new java.util.Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "123456")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    val employeesDS = spark.read
      // .schema(employeesSchema) // 从数据库读不需要~指定schema
      .jdbc(url = url, table = table, properties = properties)

    employeesDS.printSchema()
//    employeesDS.head()

    // 统计行数
    println(employeesDS.count())

    employeesDS.createOrReplaceTempView("employees")

    // 查看 salary 大于15000的人有哪些
    spark.sql(
      """
        |select *
        |from employees
        |where salary >= 15000
        |""".stripMargin
    ).show()
  }


  def readJsonSpecifySchema2(spark: SparkSession) = {
    val userFile = "data/user.json"
    import spark.implicits._
    val userDS = spark.read
      .json(userFile)
      .as[User] // DataSet[Row] => DataSet[User]
    userDS.printSchema()

    print("---------- 强制使用 schema指定类型 ---------")

    val userSchema = StructType(
      Seq(
        StructField("age", DataTypes.IntegerType),
        StructField("name", DataTypes.StringType)
      )
    )

    val userDS2 = spark.read
      .schema(userSchema)
      .json(userFile)

    /** root \|-- age: integer (nullable = true) \|-- name: string (nullable =
      * true)
      */
    userDS2.printSchema()
  }

  def readJsonSpecifySchema(spark: SparkSession) = {

    val userSchema = StructType(
      Seq(
        StructField("age", DataTypes.IntegerType),
        StructField("name", DataTypes.StringType)
      )
    )

    val userDS = spark.read
      .schema(userSchema)
      .json("data/user.json")

    /** root \|-- age: integer (nullable = true) \|-- name: string (nullable =
      * true)
      */
    userDS.printSchema()
  }

  def readJsonToWrite(spark: SparkSession) = {
    val userDS = spark.read.json("data/user.json")
    userDS.printSchema()
    userDS.createOrReplaceTempView("user")
    val user = spark.sql(
      """
        select *
        from user
        where age > 19
      """
    )
    user.write.json("data/output")
  }

  def readJson(spark: SparkSession) = {
    // 读取json文件
    /** {"age":20,"name":"qiaofeng"} {"age":19,"name":"xuzhu"}
      * {"age":18,"name":"duanyu"} {"age":22,"name":"qiaofeng"}
      */
    val userDS = spark.read.json("data/user.json")

    // show dataframe(Dataset[Row])
    /** | age |     name |
      * |:----|---------:|
      * | 20  | qiaofeng |
      * | 19  |    xuzhu |
      * | 18  |   duanyu |
      * | 22  | qiaofeng |
      * | 11  |    xuzhu |
      * | 12  |   duanyu |
      */
    userDS.show()

    println("------------ view ---------------")

    userDS.createOrReplaceTempView("user")

    // use spark sql query
    val usersparksql = spark.sql(
      """
          select *
          from user
        """
    )

    /** | age |     name |
      * |:----|---------:|
      * | 20  | qiaofeng |
      * | 19  |    xuzhu |
      * | 18  |   duanyu |
      * | 22  | qiaofeng |
      * | 11  |    xuzhu |
      * | 12  |   duanyu |
      */
    usersparksql.show()

    // query age > 18 from user
    /** | age |     name |
      * |:----|---------:|
      * | 20  | qiaofeng |
      * | 19  |    xuzhu |
      * | 22  | qiaofeng |
      */
    spark
      .sql(
        """
          select *
          from user
          where age > 18
        """
      )
      .show()

    println("------------ dsl ---------------")

    // dataset -> dsl
    userDS.select("age").show()

    userDS.printSchema()
    userDS.select("name").where("age >= 12 and age <= 18").show(false)

    userDS.select("age").groupBy("age").count().show()

    print(userDS.count())

  }

  private def runBasicDataFrameExample(spark: SparkSession) = {
    val df = spark.read.json("src/main/resources/people.json")
    // 查询数据
    df.show()

    import spark.implicits._
    // 打印元数据
    // df.printSchema()

    // 查询 name
    // df.select("name").show()

    // dsl
    // df.select('name, 'age, 'age + 1).show()

    // filter
    // df.filter('age > 21).show()

    // spark sql
    // df.createOrReplaceTempView("people")
    // val sqlDF = spark.sql("select * from people")
    // sqlDF.show()

    // global temporary view
    df.createGlobalTempView("perple")
    spark.sql("select * from global_temp.perople").show()
    spark.newSession().sql("SELECT * FROM global_temp.people").show()
  }

  private def runBasicDateSetExample(spark: SparkSession) = {
    import spark.implicits._
    // Encoders are create1d for case classes
    // val caseClassDS = Seq(Person("Andy", 21)).toDS()
    // caseClassDS.show()

    // Encoders for most common types are automatically provided by importing spark.implicits._
    // val primitiveDS = Seq(12,2,354,5).toDS()
    // primitiveDS.map(_ + 1).collect()

    // DataFrames can be converted to a Dataset by providing a class.
    // Mapping will be done by name
    val path = "src/main/resources/people.json"
    val peopleDS = spark.read.json(path).as[Person]
    peopleDS.show()
  }

}
