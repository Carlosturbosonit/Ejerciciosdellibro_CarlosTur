package Chapter7


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Random

object JoinExampleJob {

  def run(): Unit = {
    // 1️⃣ Create SparkSession
    val spark = SparkSession.builder()
      .appName("JoinExample")
      .master("local[*]") // Use all local cores
      .getOrCreate()

    import spark.implicits._ // Required for $"column" syntax

    // 2️⃣ Disable broadcast join (useful for large datasets)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    // 3️⃣ Generate sample data
    val rnd = new Random(42)

    // States and items
    val states = Map(0 -> "AZ", 1 -> "CO", 2 -> "CA", 3 -> "TX", 4 -> "NY", 5 -> "MI")
    val items = Map(0 -> "SKU-0", 1 -> "SKU-1", 2 -> "SKU-2", 3 -> "SKU-3", 4 -> "SKU-4", 5 -> "SKU-5")

    // 4️⃣ Create users DataFrame: 1 million users
    val usersDF = (0 to 1000000).map { id =>
      (id, s"user_$id", s"user_$id@databricks.com", states(rnd.nextInt(states.size)))
    }.toDF("uid", "login", "email", "user_state")

    // 5️⃣ Create orders DataFrame: 1 million orders
    val ordersDF = (0 to 1000000).map { r =>
      (r, r, rnd.nextInt(10000), 10 * r * 0.2d, states(rnd.nextInt(states.size)), items(rnd.nextInt(items.size)))
    }.toDF("transaction_id", "quantity", "users_id", "amount", "state", "items")

    // 6️⃣ Join users with orders on user ID
    val usersOrdersDF = ordersDF.join(usersDF, $"users_id" === $"uid")

    // 7️⃣ Show joined results
    usersOrdersDF.show(false)

    // 8️⃣ Stop SparkSession
    spark.stop()
  }
}
