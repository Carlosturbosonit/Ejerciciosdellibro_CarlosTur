import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, Dataset}

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import java.io.File

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.spark.sql.SparkSession

object MainApp {
  def main(args: Array[String]): Unit = {

    // Usar SparkSession centralizado
    val spark: SparkSession = Spark.getSparkSession("SparkScalaApp")

    spark.sparkContext.setLogLevel("ERROR")

    println(s"Versión de Spark: ${spark.version}")
    // Llamada a tu función de prueba
    //Chapter2.CreateScalaTest.run(spark)
    //Chapter2.AuthorsAges.run(spark)
    //Chapter1.MnMcount.run(spark)
   // Chapter3.FireIncidentsApp.run(spark)

    //Chapter3.IoTApp.run(spark)
    //Chapter3.RowExampleApp.run(spark)


    // Aquí puedes poner el resto de tu código usando `spark`
    // Por ejemplo, leer CSV, procesar DataFrame, etc.
    // Cerrar SparkSession al final
    spark.stop()
  }
}
