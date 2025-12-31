package Chapter4

import org.apache.spark.sql.SparkSession

object Framesandtables {

  def run(spark: SparkSession): Unit = {

    // ======================================
    // 1. Cargar CSV y crear vista temporal
    // ======================================
    val csvFile = "src/main/resources/Datasets/departuredelays.csv"

    val df = spark.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(csvFile)

    df.createOrReplaceTempView("us_delay_flights_tbl")

    // ======================================
    // 2. Crear Base de Datos y Tablas
    // ======================================
    spark.sql("CREATE DATABASE IF NOT EXISTS learn_spark_db")
    spark.sql("USE learn_spark_db")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS managed_us_delay_flights_tbl (
        | date STRING,
        | delay INT,
        | distance INT,
        | origin STRING,
        | destination STRING
        |)
        |""".stripMargin)

    spark.sql(
      s"""
         |CREATE TABLE IF NOT EXISTS unmanaged_us_delay_flights_tbl (
         | date STRING,
         | delay INT,
         | distance INT,
         | origin STRING,
         | destination STRING
         |)
         |USING csv
         |OPTIONS (path '$csvFile', header 'true', inferSchema 'true')
         |""".stripMargin)

    // ======================================
    // 3. Lectura de diferentes formatos
    // ======================================
    val dfParquet = spark.read
      .format("parquet")
      .load("src/main/resources/Datasets/2010-summary.parquet")

    val dfJson = spark.read
      .format("json")
      .load("src/main/resources/Datasets/json/*")

    val dfOrc = spark.read
      .format("orc")
      .load("src/main/resources/Datasets/orc/2010-summary.orc")

    // ======================================
    // 4. Escritura
    // ======================================
    df.write
      .mode("overwrite")
      .format("parquet")
      .save("/tmp/data/parquet/df_parquet")

    df.write
      .mode("overwrite")
      .saveAsTable("us_delay_flights_tbl")

    // ======================================
    // 5. Formatos especializados
    // (requiere spark-mllib)
    // ======================================
    val imageDir = "src/main/resources/Datasets/train_images"

    val imagesDF = spark.read
      .format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .option("recursiveFileLookup", "true")
      .load(imageDir)

//Preguntar a david porque estamos leyendo imagenes como binarios
    imagesDF.printSchema()

    imagesDF.select("content", "path", "length").show(3, false)

    val binaryFilesDF = spark.read
      .format("binaryFile")
      .option("pathGlobFilter", "*.jpg")
      .option("recursiveFileLookup", "true")
      .load(imageDir)
    binaryFilesDF.show(5)
    //val df = spark.read.format("avro")
    //  .load("/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*")
   // df.show(false)

    // ======================================
    // 6. Catálogo
    // ======================================
    spark.catalog.listDatabases().show(false)
    spark.catalog.listTables().show(false)
    spark.catalog.listColumns("us_delay_flights_tbl").show(false)

    println("===== SparkSQLExampleApp terminado =====")
  }
}
