package Chapter5

import com.azure.cosmos.spark._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object AzureConnect {

  def run(
           spark: SparkSession,
           accountEndpoint: String,
           masterKey: String,
           database: String,
           container: String,
           query: String,
           writeMode: SaveMode = SaveMode.Overwrite
         ): Unit = {

    // ======================================
    // 1️⃣ Crear configuración para lectura
    // ======================================
    val readConfig: Map[String, String] = Map(
      "spark.cosmos.accountEndpoint" -> accountEndpoint,
      "spark.cosmos.accountKey" -> masterKey,
      "spark.cosmos.database" -> database,
      "spark.cosmos.container" -> container,
      "spark.cosmos.read.customQuery" -> query,
      "spark.cosmos.read.partitioning.strategy" -> "Restrictive", // optional
      "spark.cosmos.read.inferSchema.enabled" -> "true"
    )

    // ======================================
    // 2️⃣ Leer datos desde Cosmos DB
    // ======================================
    val df: DataFrame = spark.read
      .format("cosmos.oltp")
      .options(readConfig)
      .load()

    println("=== Cosmos DB DataFrame ===")
    df.show(5, truncate = false)

    // ======================================
    // 3️⃣ Crear configuración para escritura
    // ======================================
    val writeConfig: Map[String, String] = Map(
      "spark.cosmos.accountEndpoint" -> accountEndpoint,
      "spark.cosmos.accountKey" -> masterKey,
      "spark.cosmos.database" -> database,
      "spark.cosmos.container" -> container,
      "spark.cosmos.write.strategy" -> "ItemOverwrite" // upsert
    )

    // ======================================
    // 4️⃣ Escritura en Cosmos DB (upsert)
    // ======================================
    df.write
      .format("cosmos.oltp")
      .options(writeConfig)
      .mode(writeMode)
      .save()

    println("=== DataFrame escrito en Cosmos DB ===")
  }
}

