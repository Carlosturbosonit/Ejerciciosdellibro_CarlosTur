package Chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.expr

object TempArrays {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._

    // ======================================
    // 1. Crear arrays de temperatura en Celsius
    // ======================================
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)

    // ======================================
    // 2. Convertir a DataFrame
    // ======================================
    val tC = Seq(t1, t2).toDF("celsius")

    // ======================================
    // 3. Crear vista temporal
    // ======================================
    tC.createOrReplaceTempView("tC")

    println("=== DataFrame de temperaturas ===")
    tC.show(false)

    // ======================================
    // 4. Convertir a Fahrenheit usando SQL
    // ======================================
    val result = spark.sql("""
      SELECT celsius, transform(celsius, t -> ((t * 9) / 5) + 32) as fahrenheit
      FROM tC
    """)

    println("=== Temperaturas en Fahrenheit ===")
    result.show(false)

   val threshold = spark.sql("""
       SELECT celsius,exists(celsius, t -> t = 38) as threshold
       FROM tC
      """)
    println("=== Threshold (existe 38?) ===")
    threshold.show(false)

    // ======================================
    // 4. Calcular promedio en Fahrenheit usando DataFrame API
    // ======================================
    val avgFahrenDF = tC.withColumn(
      "avgFahrenheit",
      expr("aggregate(celsius, 0D, (acc, t) -> acc + ((t * 9) / 5) + 32) / size(celsius)")
    )

    println("=== Promedio en Fahrenheit ===")
    avgFahrenDF.show(false)

    // Set file paths
    val delaysPath =
      "src/main/resources/Datasets/departuredelays.csv"
    val airportsPath =
      "src/main/resources/Datasets/airport-codes-na.txt"
    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")
    // Obtain departure Delays data set
    val delays = spark.read
      .option("header","true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    // Create temporary small table
    val foo = delays.filter(
      expr("""origin == 'SEA' AND destination == 'SFO' AND
 date like '01010%' AND delay > 0"""))
    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM airports_na LIMIT 10").show()
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    spark.sql("SELECT * FROM foo").show()

    // Union two tables
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
AND date LIKE '01010%' AND delay > 0""")).show()

    // In Scala
    foo.join(
      airports.as('air),
      $"air.IATA" === $"origin"
    ).select("City", "State", "date", "delay", "distance", "destination").show()


    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )

    // In Scala dropping
    val foo3 = foo2.drop("delay")
    foo3.show()

    // In Scala Renaming
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()


  }
}

