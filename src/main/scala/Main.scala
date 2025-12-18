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

    // Nivel de logs
    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._

    // Ejemplo: mostrar versión de Spark
    println(s"Versión de Spark: ${spark.version}")

    // Llamada a tu función de prueba
  //  Chapter2.CreateScalaTest.main(
   //   Array("C:\\Users\\carlos.tur\\IdeaProjects\\spark-scala-app\\src\\main\\resources\\blogs.json")
   // )

  //  Chapter1.MnMcount.main(
  //    Array.empty[String]  // MnMcount no usa args, así que podemos pasar un array vacío
  //  )
    Chapter3.FireIncidentsApp.main(Array.empty[String])


    // Aquí puedes poner el resto de tu código usando `spark`
    // Por ejemplo, leer CSV, procesar DataFrame, etc.

    // Cerrar SparkSession al final
    spark.stop()
  }
}
