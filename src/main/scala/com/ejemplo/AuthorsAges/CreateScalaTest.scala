package com.ejemplo.AuthorsAges

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._


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
      .master("local[*]")    // Ejecutar en local con todos los cores
      .getOrCreate()

    // Ruta al fichero JSON (viene de exec:java)
    val jsonFile = args(0)

    // Definir el esquema programáticamente (como en el libro)
    val schema = StructType(Array(
      StructField("Id", IntegerType, nullable = true),
      StructField("First", StringType, nullable = true),
      StructField("Last", StringType, nullable = true),
      StructField("Url", StringType, nullable = true),
      StructField("Published", StringType, nullable = true),
      StructField("Hits", IntegerType, nullable = true),
      StructField("Campaigns", ArrayType(StringType), nullable = true)
    ))

    // Leer el JSON con el esquema definido
    val blogsDF = spark.read
      .schema(schema)
      .json(jsonFile)

    // Mostrar datos
    blogsDF.show(truncate = false)

    // Mostrar el esquema
    blogsDF.printSchema()
    println(blogsDF.schema)
    
    // Ejemplo: añadir columna Big Hitters
    val blogsWithBigHittersDF =
      blogsDF.withColumn("Big Hitters", expr("Hits > 10000"))

    blogsWithBigHittersDF.show(truncate = false)
    blogsWithBigHittersDF.printSchema()

    // Parar Spark
    spark.stop()
  }
}