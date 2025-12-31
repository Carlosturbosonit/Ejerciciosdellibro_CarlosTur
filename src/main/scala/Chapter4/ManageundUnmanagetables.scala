package Chapter4

import org.apache.spark.sql.{Dataset, SparkSession}

case class TablesDelay(
                        date: String,
                        delay: Int,
                        distance: Int,
                        origin: String,
                        destination: String
                      )

object ManageundUnmanagetables {

  def run(spark: SparkSession): Unit = {
    // Path al CSV desde src
    val csvPath = "src/main/resources/Datasets/departuredelays.csv"
    // ======================================
    // 1. Crear base de datos (Hive must be enabled)
    // ======================================
    spark.sql("CREATE DATABASE IF NOT EXISTS learn_spark_db")
    spark.sql("USE learn_spark_db")

    // ======================================
    // 2. Crear managed table
    // ======================================
    spark.sql(
      """CREATE TABLE IF NOT EXISTS managed_us_delay_flights_tbl (
        |date STRING, delay INT, distance INT, origin STRING, destination STRING
        |)""".stripMargin)

    val flightsDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)

    // Guardar como managed table solo si está vacía
    if (spark.catalog.tableExists("learn_spark_db.managed_us_delay_flights_tbl")) {
      println("Managed table already exists, skipping save.")
    } else {
      flightsDF.write.mode("overwrite").saveAsTable("managed_us_delay_flights_tbl")
    }

    // ======================================
    // 3. Crear unmanaged table
    // ======================================
    spark.sql(
      s"""CREATE TABLE IF NOT EXISTS unmanaged_us_delay_flights_tbl(
         |date STRING, delay INT, distance INT, origin STRING, destination STRING
         |)
         |USING csv
         |OPTIONS (PATH '$csvPath')""".stripMargin)

    // ======================================
    // 4. Crear vistas temporales para queries rápidas
    // ======================================
    flightsDF.createOrReplaceTempView("us_delay_flights_tbl")

    // Vista temporal normal
    spark.sql(
      """CREATE OR REPLACE TEMP VIEW us_origin_airport_JFK_tmp_view AS
        |SELECT date, delay, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE origin = 'JFK'""".stripMargin)

    // Vista temporal global
    spark.sql(
      """CREATE OR REPLACE GLOBAL TEMP VIEW us_origin_airport_SFO_global_tmp_view AS
        |SELECT date, delay, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE origin = 'SFO'""".stripMargin)

    println("===== US Flight Delays Dataset Info =====")
    flightsDF.show(5, truncate = false)

    // ======================================
    // 5. Queries
    // ======================================
    println("===== Flights with distance > 1000 miles =====")
    spark.sql(
      """SELECT distance, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE distance > 1000
        |ORDER BY distance DESC""".stripMargin).show(10)

    println("===== Delays > 120 min from SFO to ORD =====")
    spark.sql(
      """SELECT date, delay, origin, destination
        |FROM us_delay_flights_tbl
        |WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD'
        |ORDER BY delay DESC""".stripMargin).show(10)

    println("===== Flight Delay Categories =====")
    spark.sql(
      """SELECT delay, origin, destination,
        | CASE
        |  WHEN delay > 360 THEN 'Very Long Delays'
        |  WHEN delay > 120 AND delay <= 360 THEN 'Long Delays'
        |  WHEN delay > 60 AND delay <= 120 THEN 'Short Delays'
        |  WHEN delay > 0 AND delay <= 60 THEN 'Tolerable Delays'
        |  WHEN delay = 0 THEN 'No Delays'
        |  ELSE 'Early'
        | END AS Flight_Delays
        |FROM us_delay_flights_tbl
        |ORDER BY origin, delay DESC""".stripMargin).show(10)

    // ======================================
    // 6. Ejemplos de uso de vistas
    // ======================================

    // Acceder a la global temp view en SQL
    println("===== Global Temp View SFO =====")
    spark.sql("SELECT * FROM global_temp.us_origin_airport_SFO_global_tmp_view").show(5)

    // Acceder a la temp view normal en SQL
    println("===== Temp View JFK =====")
    spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view").show(5)

    // Acceder a la temp view normal en Scala
    val jfkDF = spark.read.table("us_origin_airport_JFK_tmp_view")
    jfkDF.show(5)

    // ======================================
    // 7. Ejemplos para eliminar vistas
    // ======================================

    // En SQL
    spark.sql("DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view")
    spark.sql("DROP VIEW IF EXISTS global_temp.us_origin_airport_SFO_global_tmp_view")

    // En Scala
    // spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")
    // spark.catalog.dropGlobalTempView("us_origin_airport_SFO_global_tmp_view")
  }
}
//access metadata
// In Scala/Python
//spark.catalog.listDatabases()
//spark.catalog.listTables()
//spark.catalog.listColumns("us_delay_flights_tbl")
