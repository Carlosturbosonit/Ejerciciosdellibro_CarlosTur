package Chapter6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Base case class (NO cost here)
case class Usage(uid: Int, uname: String, usage: Int)

// Case class with computed field
case class UsageCost(uid: Int, uname: String, usage: Int, cost: Double)

object UsageJob {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._   // 🔑 encoders

    val r = new scala.util.Random(42)

    // Generate data
    val data = for (i <- 0 to 1000)
      yield Usage(
        i,
        "user-" + r.alphanumeric.take(5).mkString(""),
        r.nextInt(1000)
      )

    val dsUsage = spark.createDataset(data)
    dsUsage.show(10, false)

    // --------------------------------------------------
    // High Order Functions
    // --------------------------------------------------

    dsUsage
      .filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show(5, false)

    // Same filter using a named function
    def filterWithUsage(u: Usage): Boolean = u.usage > 900

    dsUsage
      .filter(filterWithUsage _)
      .orderBy(desc("usage"))
      .show(5, false)

    // --------------------------------------------------
    // map(): compute a value
    // --------------------------------------------------

    dsUsage
      .map(u => if (u.usage > 750) u.usage * 0.15 else u.usage * 0.50)
      .show(5, false)

    // Extract logic into a function
    def computeCostUsage(usage: Int): Double =
      if (usage > 750) usage * 0.15 else usage * 0.50

    dsUsage
      .map(u => computeCostUsage(u.usage))
      .show(5, false)

    // --------------------------------------------------
    // map(): return a NEW case class
    // --------------------------------------------------

    def computeUserCostUsage(u: Usage): UsageCost = {
      val cost = computeCostUsage(u.usage)
      UsageCost(u.uid, u.uname, u.usage, cost)
    }

    dsUsage
      .map(computeUserCostUsage)
      .show(5, false)
  }
}


