package Chapter4

import org.apache.spark.sql.{Dataset, SparkSession}

case class FlightDelay(
                        date: String,
                        delay: Int,
                        distance: Int,
                        origin: String,
                        destination: String
                      )

object USFlightDelaysApp {

  def run(spark: SparkSession): Unit = {
    // Path al CSV desde src
    val csvPath = "src/main/resources/Datasets/departuredelays.csv"

    // ======================================
    // 1. Leer CSV como DataFrame
    // ======================================
    val flightsDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(csvPath)

    // ======================================
    // 2. Crear vista temporal para queries
    // ======================================
    flightsDF.createOrReplaceTempView("us_delay_flights_tbl")

    println("===== US Flight Delays Dataset Info =====")
    flightsDF.show(5, truncate = false)

    // ======================================
    // 3. Queries
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
  }
}
