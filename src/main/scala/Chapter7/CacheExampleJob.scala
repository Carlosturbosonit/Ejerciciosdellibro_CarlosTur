package Chapter7



import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CacheExampleJob {

  def run(): Unit = {
    // 1️⃣ Create SparkSession
    val spark = SparkSession.builder()
      .appName("CacheExample")
      .master("local[*]") // Use all local cores
      .getOrCreate()

    import spark.implicits._ // Needed for $"column" syntax

    // 2️⃣ Create a DataFrame with 10 million records
    val df = spark.range(1L * 10000000)
      .toDF("id")                    // Rename column to "id"
      .withColumn("square", $"id" * $"id") // Add a column with id^2

    // 3️⃣ Cache the DataFrame in memory for faster access
    df.cache()

    // 4️⃣ Materialize the cache by performing an action
    val count1 = df.count()
    println(s"Count after caching (materialize cache): $count1")

    // 5️⃣ Access the DataFrame again; this time it comes from cache
    val count2 = df.count()
    println(s"Count from cache: $count2")

    //The first count() materializes the cache, whereas the second one accesses the cache,
    //resulting in a close to 12 times faster access time for this data set.
    df.createOrReplaceTempView("dfTable")
    spark.sql("CACHE TABLE dfTable")
    spark.sql("SELECT count(*) FROM dfTable").show()
//También se pueden persistir las tablas o vistas de SQL
    // (además de los DataFrames) para luego poder acceder a ellas
    // de forma más rápida.

    
    //// PERSIST EXAMPLE

    //import org.apache.spark.storage.StorageLevel
    //// Create a DataFrame with 10M records
    //val df = spark.range(1 * 10000000).toDF("id").withColumn("square", $"id" * $"id")
    //df.persist(StorageLevel.DISK_ONLY) // Serialize the data and cache it on disk
    //df.count() // Materialize the cache
    //res2: Long = 10000000
    //Command took 2.08 seconds
    //df.count() // Now get it from the cache
    //res3: Long = 10000000
    //Command took 0.38 seconds

    // 6️⃣ Stop SparkSession
    spark.stop()
  }
}

