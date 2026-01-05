package Chapter7

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object BucketedJoinJob {

  def run(): Unit = {
    // 1️⃣ Create a Spark session
    //    This is the entry point for all Spark functionality.
    //    local[*] uses all CPU cores, Hive support allows save/load tables
    val spark = SparkSession.builder()
      .appName("BucketedJoinExample")
      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._ // Enables $"columnName" syntax for DataFrame operations

    // 2️⃣ Generate example data (users and orders)
    val rnd = new scala.util.Random(42) // deterministic random generator for reproducibility
    val states = Map(0 -> "AZ", 1 -> "CO", 2 -> "CA", 3 -> "TX", 4 -> "NY", 5 -> "MI") // states mapping
    val items = Map(0 -> "SKU-0", 1 -> "SKU-1", 2 -> "SKU-2", 3 -> "SKU-3", 4 -> "SKU-4", 5 -> "SKU-5") // sample items

    // Users DataFrame: uid, login, email, user_state
    val usersDF = (0 to 1000).map { id =>
      (id, s"user_$id", s"user_$id@databricks.com", states(rnd.nextInt(states.size)))
    }.toDF("uid", "login", "email", "user_state")

    // Orders DataFrame: transaction_id, quantity, users_id, amount, state, items
    val ordersDF = (0 to 1000).map { r =>
      (r, r, rnd.nextInt(100), 10 * r * 0.2d, states(rnd.nextInt(states.size)), items(rnd.nextInt(items.size)))
    }.toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    // 3️⃣ Save usersDF and ordersDF as bucketed Parquet tables
    //    BucketBy divides data into 8 buckets by key, improving join performance
    usersDF.orderBy(asc("uid"))
      .write
      .format("parquet")       // store as efficient columnar Parquet format
      .bucketBy(8, "uid")      // create 8 buckets on uid column
      .mode(SaveMode.Overwrite) // overwrite table if it exists
      .saveAsTable("UsersTbl") // save as Hive-style managed table

    ordersDF.orderBy(asc("users_id"))
      .write
      .format("parquet")
      .bucketBy(8, "users_id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("OrdersTbl")

    // 4️⃣ Cache the tables in memory for faster access
    //    avoids reading from disk for repeated operations
    spark.sql("CACHE TABLE UsersTbl")
    spark.sql("CACHE TABLE OrdersTbl")

    // 5️⃣ Read the bucketed tables back into DataFrames
    val usersBucketDF = spark.table("UsersTbl")
    val ordersBucketDF = spark.table("OrdersTbl")

    // 6️⃣ Join the bucketed DataFrames on user id
    //    Bucketed tables improve join efficiency for large datasets
    val joinUsersOrdersBucketDF = ordersBucketDF
      .join(usersBucketDF, $"users_id" === $"uid")

    // 7️⃣ Show the joined results
    //    `false` prevents truncating columns, so you see full data
    joinUsersOrdersBucketDF.show(false)

    // 8️⃣ Stop Spark session to free resources
    spark.stop()
  }
}

