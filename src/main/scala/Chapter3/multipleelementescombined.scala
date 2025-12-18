package Chapter3

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object RowExampleApp {
  def main(args: Array[String]): Unit = {

    // Crear SparkSession
    val spark = SparkSession.builder()
      .appName("RowExample")
      .master("local[*]")
      .getOrCreate()

    // Definir esquema del DataFrame
    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("isActive", BooleanType, nullable = false),
      StructField("title", StringType, nullable = true),
      StructField("notes", StringType, nullable = true)
    ))

    // Crear un Row
    val row = Row(350, true, "Learning Spark 2E", null)

    // Crear DataFrame directamente desde Seq[Row]
    val df = spark.createDataFrame(Seq(row), schema)

    // Mostrar DataFrame completo
    println("=== DataFrame completo ===")
    df.show(false)

    // Mostrar columnas id y title
    println("=== Columnas id y title ===")
    df.select("id", "title").show(false)

    // Mostrar columnas con expresiones
    println("=== Columnas con expressions ===")
    df.select(col("id"), col("isActive"), col("title")).show(false)

    spark.stop()
  }
}

