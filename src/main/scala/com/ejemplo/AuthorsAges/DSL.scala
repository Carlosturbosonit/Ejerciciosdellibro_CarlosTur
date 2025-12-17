package com.ejemplo.AuthorsAges

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.avg

object DSL {
  def main(args: Array[String]): Unit = {
    // Crear SparkSession
    val spark = SparkSession
      .builder()
      .appName("AuthorsAges")
      .master("local[*]") // Añadido para que se ejecute localmente
      .getOrCreate()

    // Crear DataFrame con nombres y edades
    val dataDF = spark.createDataFrame(Seq(
      ("Brooke", 20),
      ("Brooke", 25),
      ("Denny", 31),
      ("Jules", 30),
      ("TD", 35)
    )).toDF("name", "age")

    // Agrupar por nombre y calcular promedio de edad
    val avgDF = dataDF.groupBy("name").agg(avg("age"))

    // Mostrar resultados
    avgDF.show()

    // Cerrar SparkSession
    spark.stop()
  }
}
