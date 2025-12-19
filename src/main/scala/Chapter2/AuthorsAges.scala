package Chapter2

import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions._

object AuthorsAges {

  // Run function to be called from MainApp
  def run(spark: SparkSession): Unit = {
    import spark.implicits._  // necesario para toDF

    // ============================
    // 1️⃣ Datos de ejemplo
    // ============================
    val data = Seq(
      ("Gabriel García Márquez", 87),
      ("Isabel Allende", 79),
      ("J.K. Rowling", 57)
    )

    // ============================
    // 2️⃣ Convertir Seq a DataFrame usando toDF
    // ============================
    val df = data.toDF("author", "age")

    println("===== Authors and Ages =====")
    df.show()

    // ============================
    // 3️⃣ Filtrar autores mayores de 60 años
    // ============================
    val mayores60 = df.filter($"age" > 60)
    println("===== Authors older than 60 =====")
    mayores60.show()
  }
}

