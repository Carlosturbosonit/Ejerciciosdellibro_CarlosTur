package Chapter3

import org.apache.spark.sql.SparkSession
import java.io.File
import org.apache.spark.sql.functions._

object FireIncidentsApp {
  def main(args: Array[String]): Unit = {

    // Comprobación de winutils
    val hadoopHome = System.getProperty("hadoop.home.dir", "C:\\hadoop")
    println(s"HADOOP_HOME: $hadoopHome")

    val winutilsPath = new File(s"$hadoopHome/bin/winutils.exe")
    println(s"Winutils existe? ${winutilsPath.exists()}")

    // Crear SparkSession (sin Hive)
    val spark = SparkSession.builder()
      .appName("FireIncidentsApp")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    // Leer CSV desde resources
    val csvPath = getClass.getResource("/Datasets/Fire_Incidents_20251217.csv").getPath
    println(s"Ruta del CSV: $csvPath")

    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(csvPath)

    // Mostrar primeras filas
    df.show(5)

    //Añadir transformations y actions
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
    // Guardar como archivo Parquet
    // =========================
    //val parquetPath = "C:/Users/carlos.tur/IdeaProjects/spark-scala-app/output/fire_incidents.parquet"
   // df.write
     // .mode("overwrite") // sobrescribe si ya existe
     // .parquet(parquetPath)
   // println(s"Archivo Parquet guardado en: $parquetPath")

    // =========================
    // Guardar como tabla Spark (en catálogo por defecto)
    // =========================
    //val parquetTable = "fire_incidents_table"
   // df.write
     // .mode("overwrite")
     // .format("parquet")
     // .saveAsTable(parquetTable)
   // println(s"Tabla Spark guardada como: $parquetTable")

    // Cerrar SparkSession
    spark.stop()
  }
}




