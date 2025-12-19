package Chapter1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MnMcount {

  // Run function to be called from MainApp
  def run(spark: SparkSession): Unit = {

    spark.sparkContext.setLogLevel("ERROR")

    // ============================
    // 1️⃣ Ruta al CSV relativa al proyecto (desde src/main/resources)
    // ============================
    val mnmFile = "src/main/resources/Datasets/mnm_dataset.csv"

    // ============================
    // 2️⃣ Leer el archivo en un DataFrame de Spark
    // ============================
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmFile)

    // ============================
    // 3️⃣ Agregar conteos de todos los colores por Estado y Color
    // ============================
    val countMnMDF = mnmDF
      .select("State", "Color", "Count")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    println("===== Total Counts by State and Color =====")
    countMnMDF.show(60)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // ============================
    // 4️⃣ Conteo agregado para California
    // ============================
    val caCountMnMDF = mnmDF
      .select("State", "Color", "Count")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .agg(count("Count").alias("Total"))
      .orderBy(desc("Total"))

    println("===== Counts for California =====")
    caCountMnMDF.show(10)
  }
}
