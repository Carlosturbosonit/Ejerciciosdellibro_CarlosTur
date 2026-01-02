package Chapter5

import org.apache.spark.sql.{SaveMode, SparkSession}

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
    // 1. Crear DataFrame desde Cosmos DB
    // ======================================
    val df = spark.read.format("cosmos.oltp")
      .option("spark.cosmos.accountEndpoint", accountEndpoint)
      .option("spark.cosmos.accountKey", masterKey)
      .option("spark.cosmos.database", database)
      .option("spark.cosmos.container", container)
      .option("spark.cosmos.read.customQuery", query)
      .load()

    println("=== Cosmos DB DataFrame ===")
    df.show(5, truncate = false)

    // ======================================
    // 2. Escritura en Cosmos DB (upsert)
    // ======================================
    df.write.format("cosmos.oltp")
      .option("spark.cosmos.accountEndpoint", accountEndpoint)
      .option("spark.cosmos.accountKey", masterKey)
      .option("spark.cosmos.database", database)
      .option("spark.cosmos.container", container)
      .mode(writeMode)
      .save()

    println("=== DataFrame escrito en Cosmos DB ===")
  }
}
