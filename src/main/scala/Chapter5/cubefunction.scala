package Chapter5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

object CubeFunction {

  def run(spark: SparkSession): Unit = {
    // ======================================
    // 1. Definir la función de cubo
    // ======================================
    val cubed = (s: Long) => s * s * s

    // ======================================
    // 2. Registrar la UDF en Spark SQL
    // ======================================
    spark.udf.register("cubed", cubed)

    // ======================================
    // 3. Crear una vista temporal para probar
    // ======================================
    spark.range(1, 9).createOrReplaceTempView("udf_test")

    // ======================================
    // 4. Consultar la UDF en Spark SQL
    // ======================================
    spark.sql("SELECT id, cubed(id) AS id_cubed FROM udf_test").show()
  }
}

