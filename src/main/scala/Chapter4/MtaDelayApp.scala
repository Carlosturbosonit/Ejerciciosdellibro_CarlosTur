package Chapter4

import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

// Case class con los nombres correctos
case class MtaDelay(
                     Service_Date: String,
                     Train: String,
                     Branch: String,
                     Location_Name: String,
                     Depart_Station: String,
                     Depart_Time: String,
                     Arrive_Station: String,
                     Arrive_Time: String,
                     Period: String,
                     Status: String,
                     Minutes_Late: Option[Int],
                     Delay_Category: String
                   )

object MtaDelaysApp {

  // Función para leer CSV y normalizar nombres de columna
  def readCSV(spark: SparkSession, path: String): Dataset[MtaDelay] = {
    import spark.implicits._

    val rawDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)

    val df = rawDF
      .withColumnRenamed("Service Date", "Service_Date")
      .withColumnRenamed("Location Name", "Location_Name")
      .withColumnRenamed("Depart Station", "Depart_Station")
      .withColumnRenamed("Depart Time", "Depart_Time")
      .withColumnRenamed("Arrive Station", "Arrive_Station")
      .withColumnRenamed("Arrive Time", "Arrive_Time")
      .withColumnRenamed("Minutes Late", "Minutes_Late")
      .withColumnRenamed("Delay Category", "Delay_Category")

    df.as[MtaDelay]
  }

  // Crear vista temporal para consultas SQL
  def createTempView(ds: Dataset[MtaDelay], viewName: String): Unit = {
    ds.createOrReplaceTempView(viewName)
    println(s"Temporary view '$viewName' created.")
  }

  // Función principal para ejecutar el flujo
  def run(spark: SparkSession): Unit = {
    val csvPath = "src/main/resources/Datasets/MTA_Metro-North_Delays__Beginning_2012.csv"

    // Leer CSV
    val ds = readCSV(spark, csvPath)

    // Mostrar las primeras 10 filas
    println("===== First 10 rows =====")
    ds.show(10, truncate = false)

    // Crear vista SQL
    createTempView(ds, "mta_delays_tbl")

    // Ejemplo SQL: promedio de minutos de retraso por estación de salida
    println("===== Average Minutes Late by Depart Station =====")
    val avgDelaysDF = spark.sql(
      """
        |SELECT Depart_Station, AVG(Minutes_Late) AS avg_late
        |FROM mta_delays_tbl
        |GROUP BY Depart_Station
        |ORDER BY avg_late DESC
        |""".stripMargin)
    avgDelaysDF.show(10, truncate = false)

    // Ejemplo Dataset API: filtrar retrasos mayores a 60 minutos
    val longDelaysDS = ds.filter(_.Minutes_Late.exists(_ > 60))
    println("===== Delays greater than 60 minutes =====")
    longDelaysDS.show(10, truncate = false)
  }
}

