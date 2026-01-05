package Chapter7

import org.apache.spark.sql.SparkSession

object SparkConfigJob {

  /**
   * Run the Spark configuration demo
   * Creates a SparkSession, prints configs, updates shuffle partitions safely,
   * then prints configs again.
   */
  def run(): Unit = {
    // 1️⃣ Create a SparkSession with some initial settings
    val spark = SparkSession.builder()
      .appName("SparkConfig")
      .master("local[*]")                         // Use all local cores
      .config("spark.sql.shuffle.partitions", 5)  // Initial shuffle partitions
      .config("spark.executor.memory", "2g")      // Executor memory
      .getOrCreate()

    println("****** Initial Spark Configurations ******")
    printConfigs(spark)

    // 2️⃣ Safely update a config: spark.sql.shuffle.partitions
    val configName = "spark.sql.shuffle.partitions"

    if (spark.conf.isModifiable(configName)) {
      val newValue = spark.sparkContext.defaultParallelism
      spark.conf.set(configName, newValue)
      println(s"****** Updated $configName to default parallelism ($newValue) ******")
    } else {
      println(s"****** Config $configName is NOT modifiable ******")
    }

    // 3️⃣ Print configs again to confirm changes
    println("****** Spark Configurations After Update ******")
    printConfigs(spark)

    // 4️⃣ Stop the Spark session
    spark.stop()
  }

  /**
   * Helper function to print all Spark configurations
   */
  def printConfigs(session: SparkSession): Unit = {
    val configs = session.conf.getAll
    for (k <- configs.keySet) {
      println(s"$k -> ${configs(k)}")

      // Enables dynamic allocation of executors: Spark can add/remove executors at runtime based on workload
      // spark.dynamicAllocation.enabled -> true

      // Minimum number of executors Spark will keep running even if they are idle
      // spark.dynamicAllocation.minExecutors -> 2

      // Time to wait before requesting new executors when there are pending tasks in the queue
      // spark.dynamicAllocation.schedulerBacklogTimeout -> 1m  // 1 minute

      // Maximum number of executors Spark can request dynamically
      // spark.dynamicAllocation.maxExecutors -> 20

      // Time an executor can stay idle before Spark removes it
      // spark.dynamicAllocation.executorIdleTimeout -> 2min  // 2 minutes
//
    }
  }
}

