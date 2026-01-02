package Chapter5
//corregir eeste 
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.expr

object JoinDelay {

  def run(spark: SparkSession, foo: DataFrame, delays: DataFrame, airports: DataFrame): Unit = {

    import spark.implicits._

    // ======================================
    // 1. Unión de dos tablas
    // ======================================
    val bar = delays.union(foo)

    // ======================================
    // 2. Join entre foo y airports
    // ======================================
    val joinedDF = foo.join(
      airports.as("air"),
      $"air.IATA" === $"origin"
    ).select("City", "State", "date", "delay", "distance", "destination")

    println("=== Joined DataFrame ===")
    joinedDF.show()

    // ======================================
    // 3. Añadir columna basada en CASE
    // ======================================
    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )

    // ======================================
    // 4. Eliminar columna 'delay'
    // ======================================
    val foo3 = foo2.drop("delay")

    // ======================================
    // 5. Renombrar columna 'status' a 'flight_status'
    // ======================================
    val foo4 = foo3.withColumnRenamed("status", "flight_status")

    println("=== Final DataFrame ===")
    foo4.show()
  }
}
