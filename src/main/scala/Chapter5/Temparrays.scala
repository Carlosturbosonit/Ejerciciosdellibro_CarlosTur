package Chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

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
  }
}

