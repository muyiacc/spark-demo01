package cc.seektao.spark.sql.exer

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Exer01 {
  // 定义 case class 表示模型的结构
  case class GeminiModel(
      name: String,
      version: String,
      displayName: String,
      description: String,
      inputTokenLimit: Long,
      outputTokenLimit: Long,
      supportedGenerationMethods: Seq[String],
      temperature: Double,
      topK: Long,
      topP: Double
  )

  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val conf = new SparkConf()
      .setAppName("Read from JSON")
      .setMaster("local[*]")

    val spark = SparkSession
      .builder()
      .config(conf)
      .getOrCreate()

    import spark.implicits._

    // 读取 JSON 文件
    val geminiModulePath = "src/main/resources/gemini_models.json"
    val geminiModuleDF =
      spark.read.option("multiLine", true).json(geminiModulePath)

    // 展开 models 数组并映射到 case class
    val explodedDF = geminiModuleDF
      .select(explode(col("models")).as("model"))
      .select("model.*") // 自动展开所有字段
      .as[GeminiModel] // 将 DataFrame 转换为 Dataset

    // 打印 Schema 和数据
    // explodedDF.printSchema()
    // explodedDF.show(false)

    // 注册临时视图以支持 SQL 查询
    explodedDF.createOrReplaceTempView("gemini_models")

    // 示例：使用 SQL 查询所有字段
    // val resultDF = spark.sql(
    //   """
    //     SELECT
    //       name,
    //       version,
    //       displayName,
    //       description,
    //       inputTokenLimit,
    //       outputTokenLimit,
    //       supportedGenerationMethods,
    //       temperature,
    //       topK,
    //       topP
    //     FROM
    //       gemini_models
    //   """
    // )
    // resultDF.show(false)

    // 示例：灵活查询特定字段
    // val specificFieldsDF = spark.sql(
    //   """
    //     SELECT
    //       name,
    //       description,
    //       temperature
    //     FROM
    //       gemini_models
    //   """
    // )
    // specificFieldsDF.show(false)

    // 查询
    spark
      .sql(
        """
            select
                name
            from
                gemini_models
            where
                name like '%gemini-2.0%'
                and inputTokenLimit > 1000000
                and outputTokenLimit >= 4096
        """
      )
      .show(false)

    // 导出数据
    // val allModelsDS = spark.sql(
    //   """
    //     select
    //         *
    //     from
    //        gemini_models
    //   """
    // )
    // val flattenedDS = allModelsDS.withColumn(
    //   "supportedGenerationMethods",
    //   concat_ws(",", col("supportedGenerationMethods")) // 将数组转为逗号分隔的字符串
    // )
    // flattenedDS.write
    //   .option("header", true)
    //   .mode("overwrite")
    //   .csv("output")

    // 停止 SparkSession
    spark.close()
  }
}
