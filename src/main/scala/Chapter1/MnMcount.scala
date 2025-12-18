package Chapter1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    // Ruta fija al CSV
    val mnmFile = "C:\\Users\\carlos.tur\\IdeaProjects\\spark-scala-app\\src\\main\\resources\\Datasets\\mnm_dataset.csv"

    // Leer el archivo en un DataFrame de Spark
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // Agregar conteos de todos los colores por Estado y Color
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    // Mostrar resultados
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // Conteo agregado para California
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    // Mostrar resultados para California
    caCountMnMDF.show(10)

    // Detener SparkSession
    spark.stop()
  }
}
