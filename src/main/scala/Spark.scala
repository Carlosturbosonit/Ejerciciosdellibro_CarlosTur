import org.apache.spark.sql.SparkSession

object Spark {

  def getSparkSession(
                       name: String = "spark-scala-app",
                       hive: Boolean = true
                     ): SparkSession = {

    val sparkLocalDir = "C:/Users/carlos.tur/tmp/spark"
    val warehouseDir  = "C:/Users/carlos.tur/tmp/spark-warehouse"
    val hiveScratch   = "C:/Users/carlos.tur/tmp/hive/spark-scratch"
    val hiveResources = "C:/Users/carlos.tur/tmp/hive/resources"
    val hadoopTmp     = "C:/Users/carlos.tur/tmp/hadoop"

    // Crear carpetas si no existen
    Seq(sparkLocalDir, warehouseDir, hiveScratch, hiveResources, hadoopTmp).foreach { path =>
      val dir = new java.io.File(path)
      if (!dir.exists()) dir.mkdirs()
    }

    val builder = SparkSession.builder()
      .appName(name)
      .master("local[*]")
      .config("spark.local.dir", sparkLocalDir)
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.hadoop.tmp.dir", hadoopTmp)
      .config("spark.hadoop.hive.exec.scratchdir", hiveScratch)
      .config("spark.hadoop.hive.downloaded.resources.dir", hiveResources)
      .config("spark.hadoop.io.native.lib.available", "false")

      // ✅ Configuración Delta
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    if (hive) builder.enableHiveSupport().getOrCreate()
    else builder.getOrCreate()
  }
}
