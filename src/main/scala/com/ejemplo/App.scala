package com.ejemplo

import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Uso: mvn exec:java \"-Dexec.args=<archivo>\"")
      System.exit(1)
    }

    val file = args(0)

    val spark = SparkSession.builder
      .appName("WordCount")
      .master("local[*]")
      .getOrCreate()

    println("Leyendo archivo: " + file)

    val sc = spark.sparkContext

    val text = sc.textFile(file)

    val counts = text
      .flatMap(_.split("\\W+"))
      .filter(_.nonEmpty)
      .map(_.toLowerCase)
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.collect().foreach(println)

    spark.stop()
  }
}


