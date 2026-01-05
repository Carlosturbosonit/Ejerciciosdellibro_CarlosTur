package Chapter6

import org.apache.spark.sql.SparkSession


case class Bloggers(
                     id: Long,
                     first: String,
                     last: String,
                     url: String,
                     date: String,
                     hits: Long,
                     campaigns: Array[String]
                   )

object Bloggers {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    val bloggersPath = "src/main/resources/blogs.json"

    val bloggersDS = spark
      .read
      .format("json")
      .load(bloggersPath)
      // map JSON fields → case class fields
      .withColumnRenamed("Id", "id")
      .withColumnRenamed("First", "first")
      .withColumnRenamed("Last", "last")
      .withColumnRenamed("Url", "url")
      .withColumnRenamed("Published", "date")
      .withColumnRenamed("Hits", "hits")
      .withColumnRenamed("Campaigns", "campaigns")
      .as[Bloggers]

    bloggersDS.show(false)

  }
}

