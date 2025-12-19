package Chapter3

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object RowExampleApp {

  def run(spark: SparkSession): Unit = {

    // Definir esquema del DataFrame
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("isActive", BooleanType, nullable = false),
      StructField("title", StringType, nullable = true),
      StructField("notes", StringType, nullable = true)
    ))

    // Crear un Row
    val row = Row(350, true, "Learning Spark 2E", null)

    // ✅ CORRECTO: Seq -> RDD
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(row)),
      schema
    )

    // Mostrar DataFrame completo
    println("=== DataFrame completo ===")
    df.show(false)

    // Mostrar columnas id y title
    println("=== Columnas id y title ===")
    df.select("id", "title").show(false)

    // Mostrar columnas con expresiones
    println("=== Columnas con expressions ===")
    df.select(col("id"), col("isActive"), col("title")).show(false)
  }
}


