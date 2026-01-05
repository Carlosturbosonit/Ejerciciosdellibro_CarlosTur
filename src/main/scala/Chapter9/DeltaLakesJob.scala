package Chapter9

import org.apache.spark.sql.{SaveMode, SparkSession}
import io.delta.tables._
import org.apache.spark.sql.functions._

object DeltaLakesJob {

  def run(spark: SparkSession): Unit = {

    import spark.implicits._

    // Paths
    val sourcePath = "src/main/resources/Datasets/loan-risks.snappy.parquet"
    val deltaPath  = "/tmp/loans_delta"
    val checkpointDir = "/tmp/loans_delta_checkpoints"

    // Guardar Parquet como Delta
    spark.read
      .format("parquet")
      .load(sourcePath)
      .write
      .format("delta")
      .mode(SaveMode.Overwrite)
      .save(deltaPath)

    // Crear vista temporal
    spark.read.format("delta").load(deltaPath).createOrReplaceTempView("loans_delta")

    spark.sql("SELECT count(*) AS total_loans FROM loans_delta").show()
    spark.sql("SELECT * FROM loans_delta LIMIT 5").show()

    // Nuevas filas
    val loanUpdates = Seq(
      (1111111L, 1000, 1000.0, "TX", false),
      (2222222L, 2000, 0.0, "CA", true)
    ).toDF("loan_id", "funded_amnt", "paid_amnt", "addr_state", "closed")

    loanUpdates.write
      .format("delta")
      .mode("append")
      .save(deltaPath)

    val deltaTable = DeltaTable.forPath(spark, deltaPath)

    // UPDATE seguro
    deltaTable.update(
      condition = col("addr_state") === "OR",
      set = Map("addr_state" -> lit("WA"))
    )

    // DELETE
    deltaTable.delete("funded_amnt >= paid_amnt")

    // MERGE
    deltaTable.alias("t")
      .merge(loanUpdates.alias("s"), "t.loan_id = s.loan_id")
      .whenMatched.updateAll()
      .whenNotMatched.insertAll()
      .execute()

    deltaTable.history(5).select("version", "timestamp", "operation").show(false)

    // Mostrar tabla final
    spark.read.format("delta").load(deltaPath).show(false)
  }
}

