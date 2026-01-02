package Chapter5

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

object JDBCConnect {

  def run(
           spark: SparkSession,
           url: String,
           dbTable: String,
           user: String,
           password: String
         ): Unit = {

    // ======================================
    // 1. Leer usando load() con driver explícito
    // ======================================
    val jdbcDF1: DataFrame = spark
      .read
      .format("jdbc")
      .option("url", url)
      .option("dbtable", dbTable)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver") // PostgreSQL driver
      .load()

    println("=== JDBC DF usando load() ===")
    jdbcDF1.show(5, false)

    // ======================================
    // 2. Leer usando jdbc() con Properties
    // ======================================
    val cxnProp = new Properties()
    cxnProp.put("user", user)
    cxnProp.put("password", password)
    cxnProp.put("driver", "org.postgresql.Driver") // PostgreSQL driver

    val jdbcDF2: DataFrame = spark
      .read
      .jdbc(url, dbTable, cxnProp)

    println("=== JDBC DF usando jdbc(Properties) ===")
    jdbcDF2.show(5, false)

    // ======================================
    // 3. Escribir usando save() con driver
    // ======================================
    jdbcDF1
      .write
      .format("jdbc")
      .option("url", url)
      .option("dbtable", dbTable)
      .option("user", user)
      .option("password", password)
      .option("driver", "org.postgresql.Driver") // PostgreSQL driver
      .mode("overwrite")
      .save()

    println("=== DataFrame escrito en la base de datos ===")
  }
}

