package Chapter2

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

object AuthorsAges {
  def main(args: Array[String]): Unit = {
    // Crear SparkSession
    val spark = SparkSession.builder()
      .appName("AuthorsAges")
      .master("local[*]") // usar local para pruebas
      .getOrCreate()

    import spark.implicits._  // necesario para toDF

    // Datos de ejemplo: tupla (autor, edad)
    val data = Seq(
      ("Gabriel García Márquez", 87),
      ("Isabel Allende", 79),
      ("J.K. Rowling", 57)
    )

    // Convertir Seq a DataFrame usando toDF
    val df = data.toDF("author", "age")

    // Mostrar el DataFrame
    df.show()

    // Ejemplo: filtrar autores mayores de 60 años
    val mayores60 = df.filter($"age" > 60)
    mayores60.show()

    // Parar Spark
    spark.stop()
  }
}
