package Chapter2

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

object CreateScalaTest {

  // Run function to be called from MainApp
  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    // ============================
    // 1️⃣ Generar JSON internamente
    // ============================
    val jsonPath = generateJSON()

    // ============================
    // 2️⃣ Definir esquema
    // ============================
    val schema = StructType(Array(
      StructField("Id", IntegerType, nullable = true),
      StructField("First", StringType, nullable = true),
      StructField("Last", StringType, nullable = true),
      StructField("Url", StringType, nullable = true),
      StructField("Published", StringType, nullable = true),
      StructField("Hits", IntegerType, nullable = true),
      StructField("Campaigns", ArrayType(StringType), nullable = true)
    ))

    // ============================
    // 3️⃣ Leer JSON generado
    // ============================
    val blogsDF = spark.read
      .schema(schema)
      .json(jsonPath)

    println("===== Blogs DataFrame =====")
    blogsDF.show(truncate = false)
    blogsDF.printSchema()

    // ============================
    // 4️⃣ Crear un DataFrame de autores
    // ============================
    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    val authorsDF = rows.toDF("Author", "State")
    println("===== Authors DataFrame =====")
    authorsDF.show()

    // ============================
    // 5️⃣ Añadir columna Big Hitters
    // ============================
    val blogsWithBigHittersDF =
      blogsDF.withColumn("Big Hitters", expr("Hits > 10000"))

    println("===== Blogs with Big Hitters =====")
    blogsWithBigHittersDF.show(truncate = false)

    // ============================
    // 6️⃣ Concatenar columnas
    // ============================
    println("===== AuthorsId Column =====")
    blogsWithBigHittersDF
      .withColumn("AuthorsId", concat(col("First"), col("Last"), col("Id")))
      .select(col("AuthorsId"))
      .show(4)

    // ============================
    // 7️⃣ Diferentes formas de acceder a una columna
    // ============================
    println("===== Access Hits Column =====")
    blogsWithBigHittersDF.select(expr("Hits")).show(2)
    blogsWithBigHittersDF.select(col("Hits")).show(2)
    blogsWithBigHittersDF.select("Hits").show(2)

    // ============================
    // 8️⃣ Ordenar por Id descendente
    // ============================
    println("===== Sorted by Id Desc =====")
    blogsWithBigHittersDF.sort(col("Id").desc).show()
  }

  // Función que genera JSON de prueba y devuelve la ruta
  private def generateJSON(): String = {
    val data = Seq(
      """{"Id":1,"First":"Matei","Last":"Zaharia","Url":"https://tinyurl.1","Published":"3/1/2015","Hits":12345,"Campaigns":["twitter","LinkedIn"]}""",
      """{"Id":2,"First":"Reynold","Last":"Xin","Url":"https://tinyurl.2","Published":"3/2/2015","Hits":25568,"Campaigns":["facebook"]}""",
      """{"Id":3,"First":"Josh","Last":"Rosen","Url":"https://tinyurl.3","Published":"3/3/2015","Hits":9876,"Campaigns":["linkedin"]}"""
    )

    val path = "C:/temp/blogs.json"
    Files.write(Paths.get(path), data.mkString("\n").getBytes(StandardCharsets.UTF_8))
    println(s"Archivo JSON generado en: $path")
    path
  }
}

