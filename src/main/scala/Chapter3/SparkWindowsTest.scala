package Chapter3

import java.io.File
import org.apache.spark.sql.SparkSession

object SparkWindowsParquet {
  def main(args: Array[String]): Unit = {

    // Inicializa SparkSession
    val spark = SparkSession.builder()
      .appName("SparkWindowsParquet")
      .master("local[*]")  // Ejecuta en modo local con todos los cores
      .getOrCreate()

    // Configuración crítica para Windows: evita el uso de NativeIO/Winutils
    spark.conf.set("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    spark.conf.set("spark.hadoop.fs.AbstractFileSystem.file.impl", "org.apache.hadoop.fs.local.Local")
    spark.conf.set("spark.hadoop.fs.file.impl.disable.cache", "true")

    import spark.implicits._

    // Crea un DataFrame de ejemplo
    val df = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("id", "name")

    // Directorio de salida
    val outputPath = "C:/temp/spark_parquet"

    // Crear directorio si no existe
    val dir = new File(outputPath)
    if (!dir.exists()) dir.mkdirs()

    // Escribir DataFrame en Parquet
    try {
      df.write.mode("overwrite").parquet(outputPath)
      println(s"Parquet guardado correctamente en: $outputPath")
    } catch {
      case e: Exception =>
        println("Error al guardar Parquet: " + e.getMessage)
        e.printStackTrace()
    }

    // Leer Parquet para probar
    try {
      val readDf = spark.read.parquet(outputPath)
      readDf.show()
    } catch {
      case e: Exception =>
        println("Error al leer Parquet: " + e.getMessage)
        e.printStackTrace()
    }

    spark.stop()
  }
}






