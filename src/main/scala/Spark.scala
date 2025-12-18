import org.apache.spark.sql.SparkSession
object Spark {
  def getSparkSession(name: String = "spark-scala-app", hive: Boolean = false):SparkSession ={
    if(hive){
      SparkSession
        .builder()
        .master("local[*]")
        .appName(name)
        .config("spark.some.config.option","some-value")
        .enableHiveSupport()
        .getOrCreate()
    } else {
      SparkSession
        .builder()
        .master("local[*]")
        .appName(name)
        .config("spark.some.config.option","some-value")
        .getOrCreate()
    }
  }
}
