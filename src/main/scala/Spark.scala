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
      // Para spark.sql.warehouse.dir, aquí usamos path normal
      .config("spark.sql.warehouse.dir", warehouseDir)
      .config("spark.hadoop.tmp.dir", hadoopTmp)
      .config("spark.hadoop.hive.exec.scratchdir", hiveScratch)
      .config("spark.hadoop.hive.downloaded.resources.dir", hiveResources)
      // Evita NativeIO en Windows
      .config("spark.hadoop.io.native.lib.available", "false")

    if (hive) builder.enableHiveSupport().getOrCreate()
    else builder.getOrCreate()
  }
}
