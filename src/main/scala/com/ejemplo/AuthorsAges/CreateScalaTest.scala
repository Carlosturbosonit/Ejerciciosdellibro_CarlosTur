package com.ejemplo.AuthorsAges

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row


object CreateScalaTest {

  def main(args: Array[String]): Unit = {

    // Comprobar argumentos
    if (args.length <= 0) {
      println("Usage: CreateScalaTest <path-to-blogs.json>")
      System.exit(1)
    }

    // Crear SparkSession
    val spark = SparkSession
      .builder()
      .appName("Example-3_7")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val jsonFile = args(0)

    // Definir el esquema
    val schema = StructType(Array(
      StructField("Id", IntegerType, nullable = true),
      StructField("First", StringType, nullable = true),
      StructField("Last", StringType, nullable = true),
      StructField("Url", StringType, nullable = true),
      StructField("Published", StringType, nullable = true),
      StructField("Hits", IntegerType, nullable = true),
      StructField("Campaigns", ArrayType(StringType), nullable = true)
    ))

    val blogRow = Row(
      6,
      "Reynold",
      "Xin",
      "https://tinyurl.6",
      "3/2/2015",              // Published
      255568,                  // Hits
      Array("twitter", "LinkedIn")
    )

    // Imprimir toda la Row
    println(blogRow)

    // Imprimir un campo individual (por ejemplo, First)
    println(blogRow(1))  // Reynold

    // También puedes recorrer todos los campos
    blogRow.toSeq.foreach(f => println(f))

    // ============================
    // 1️⃣ LEER EL JSON
    // ============================
    val blogsDF = spark.read
      .schema(schema)
      .json(jsonFile)

    blogsDF.show(truncate = false)
    blogsDF.printSchema()


    val rows = Seq(("Matei Zaharia", "CA"), ("Reynold Xin", "CA"))
    val authorsDF = rows.toDF("Author", "State")
    authorsDF.show()

    // ============================
    // 2️⃣ AÑADIR COLUMNA Big Hitters
    // ============================
    val blogsWithBigHittersDF =
      blogsDF.withColumn("Big Hitters", expr("Hits > 10000"))

    blogsWithBigHittersDF.show(truncate = false)

    // ============================
    // 3️⃣ CONCATENAR COLUMNAS
    // ============================
    blogsWithBigHittersDF
      .withColumn(
        "AuthorsId",
        concat(col("First"), col("Last"), col("Id"))
      )
      .select(col("AuthorsId"))
      .show(4)

    // ============================
    // 4️⃣ DIFERENTES FORMAS DE ACCEDER A UNA COLUMNA
    // ============================
    blogsWithBigHittersDF.select(expr("Hits")).show(2)
    blogsWithBigHittersDF.select(col("Hits")).show(2)
    blogsWithBigHittersDF.select("Hits").show(2)

    // ============================
    // 5️⃣ ORDENAR POR Id DESCENDENTE
    // ============================
    blogsWithBigHittersDF.sort(col("Id").desc).show()
    blogsWithBigHittersDF.sort($"Id".desc).show()

    // ============================
    // 6️⃣ PARAR SPARK
    // ============================
    spark.stop()
  }
}
