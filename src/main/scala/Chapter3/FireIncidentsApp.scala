package Chapter3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{functions => F}
import java.util.Arrays

object FireIncidentsApp {

  def run(spark: SparkSession): Unit = {

    import spark.implicits._

    // =========================
    // Leer CSV (path relativo)
    // =========================
    val csvPath = "src/main/resources/Datasets/Fire_Incidents_20251217.csv"
    println(s"Ruta del CSV: $csvPath")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Mostrar primeras filas
    df.show(5, false)

    // =========================
    // Transformations & Actions
    // =========================
    df
      .select("Address")
      .where(col("Address").isNotNull)
      .agg(countDistinct(col("Address")).alias("DistinctAddress"))
      .show()

    df
      .select("Address")
      .where($"Address".isNotNull)
      .distinct()
      .show(10, false)

    // =========================
    // Renaming
    // =========================
    val newFireDF =
      df.withColumnRenamed("Incident Number", "IncidentNumberGreaterThan5")

    newFireDF
      .select("IncidentNumberGreaterThan5")
      .where($"IncidentNumberGreaterThan5" > 5)
      .show(5, false)

    // =========================
    // Fechas y timestamps
    // =========================
    val fireTsDF = df
      .withColumn(
        "IncidentDate",
        to_timestamp(col("Incident Date"), "yyyy/MM/dd")
      )
      .drop("Incident Date")

    fireTsDF.show(5, false)

    fireTsDF
      .select("IncidentDate")
      .show(5, false)

    fireTsDF
      .select(year($"IncidentDate").alias("Year"))
      .distinct()
      .orderBy("Year")
      .show()

    fireTsDF
      .select(
        F.sum("Fire Injuries").alias("TotalFireInjuries"),
        F.avg("Suppression Units").alias("AvgSuppressionUnits"),
        F.min("Suppression Units").alias("MinSuppressionUnits"),
        F.max("Suppression Units").alias("MaxSuppressionUnits")
      )
      .show()

    // =========================
    // Guardar como Parquet (path relativo)
    // =========================
    val parquetPath = "Chapter3/fire_incidents_parquet"

    fireTsDF.write
      .mode("overwrite")
      .parquet(parquetPath)

    println(s"Parquet guardado en: $parquetPath")
  }
}


