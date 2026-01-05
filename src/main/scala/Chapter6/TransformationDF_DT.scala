package Chapter6

// DataFrame df to a Dataset of type SomeCaseClass,
//simply use the df.as[SomeCaseClass] notation.
import org.apache.spark.sql.SparkSession

object TransformationDF_DT {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._
    val bloggersPath = "src/main/resources/blogs.json"
    val bloggersDS = spark
      .read
      .format("json")
      .option("multiLine", false) // default false
      .load(bloggersPath)


    bloggersDS.show(10, false)
  }
}

