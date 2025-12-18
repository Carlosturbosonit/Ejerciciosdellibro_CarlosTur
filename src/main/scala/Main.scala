import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, Dataset}

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import java.io.File

object MainApp {
  def main(args: Array[String]): Unit = {

    // Obtener SparkSession usando la función por defecto de Spark.scala
    val spark: SparkSession = Spark.getSparkSession()
    import spark.implicits._
    // Ejemplo: mostrar versión de Spark
    println(s"Versión de Spark: ${spark.version}")

    // Aquí puedes poner el resto de tu código usando `spark`
    // Por ejemplo, leer CSV, procesar DataFrame, etc.

    // Cerrar SparkSession al final
    spark.stop()
  }
}
