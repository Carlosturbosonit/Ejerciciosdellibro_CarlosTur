import org.apache.spark.sql.SparkSession

object Spark {

  def getSparkSession(
                       name: String = "spark-scala-app",
                       hive: Boolean = false
                     ): SparkSession = {

    val builder = SparkSession
      .builder()
      .master("local[*]")
      .appName(name)
      // Evita problemas de Hadoop NativeIO en Windows
      .config("spark.hadoop.io.native.lib.available", "false")

    if (hive) {
      builder
        .enableHiveSupport()
        .getOrCreate()
    } else {
      builder.getOrCreate()
    }
  }
}

