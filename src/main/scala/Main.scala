import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.{SparkSession, DataFrame, SaveMode}

import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.avg
import java.io.File

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

import org.apache.spark.sql.SparkSession

object MainApp {
  def main(args: Array[String]): Unit = {
    // === 1️⃣ Configuración de Hadoop antes de Spark ===
    val hadoopHome = "C:\\hadoop"
    val hadoopBin  = hadoopHome + "\\bin"

    System.setProperty("hadoop.home.dir", hadoopHome)
    System.setProperty("spark.local.dir", "C:\\Users\\carlos.tur\\tmp\\spark")

    println("java.version=" + System.getProperty("java.version"))
    println("java.vendor=" + System.getProperty("java.vendor"))
    println("os.arch=" + System.getProperty("os.arch"))
    println("Hadoop jars version: " + org.apache.hadoop.util.VersionInfo.getVersion)
    println("hadoop.home.dir(sysprop): " + System.getProperty("hadoop.home.dir"))
    println("java.library.path: " + System.getProperty("java.library.path"))
    println("HADOOP_HOME(env): " + sys.env.getOrElse("HADOOP_HOME", "<no>"))
    println("Path has bin? " + sys.env.getOrElse("Path","").toLowerCase.contains(hadoopBin.toLowerCase))
    println("java.io.tmpdir=" + System.getProperty("java.io.tmpdir"))
    println("user.dir=" + System.getProperty("user.dir"))
    println("TEMP=" + sys.env.getOrElse("TEMP","<no>"))
    println("TMP=" + sys.env.getOrElse("TMP","<no>"))

    println("scala-library location = " + classOf[scala.Option[_]].getProtectionDomain.getCodeSource.getLocation)
    println("scala-collection-compat? = " + Option(classOf[scala.collection.Iterable[_]].getProtectionDomain.getCodeSource).map(_.getLocation).getOrElse("n/a"))


    import scala.sys.process._

    println("java.version=" + System.getProperty("java.version"))
    println("os.arch=" + System.getProperty("os.arch"))
    println("Path=" + sys.env.getOrElse("Path","<no Path>"))

    def runWhere(name: String): Unit = {
      val cmd = Seq("cmd", "/c", "where", name)
      val out = cmd.!!.trim
      println(s"where $name =>\n$out")
    }

    runWhere("msvcr120.dll")
    runWhere("msvcp120.dll")

    // === 2️⃣ Crear SparkSession con Hive habilitado ===
    val spark: SparkSession = Spark.getSparkSession("SparkScalaApp", hive = true)
    import spark.implicits._
    spark.sparkContext.setLogLevel("ERROR")
    println("CONF hive.downloaded.resources.dir = " +
      spark.sparkContext.hadoopConfiguration.get("hive.downloaded.resources.dir"))

    println("CONF hive.exec.scratchdir = " +
      spark.sparkContext.hadoopConfiguration.get("hive.exec.scratchdir"))

    println("spark.hadoop.hive.downloaded.resources.dir (spark.conf) = " +
      spark.conf.get("spark.hadoop.hive.downloaded.resources.dir", "<none>"))

    println(s"Versión de Spark: ${spark.version}")
    // Usar SparkSession centralizado con Hive habilitado

    // Llamada a tu función de prueba
    //Chapter2.CreateScalaTest.run(spark)
    //Chapter2.AuthorsAges.run(spark)
    //Chapter1.MnMcount.run(spark)
   // Chapter3.FireIncidentsApp.run(spark)

    //Chapter3.IoTApp.run(spark)
    //Chapter3.RowExampleApp.run(spark)
    //Chapter4.MtaDelaysApp.run(spark)
    //Chapter4.USFlightDelaysApp.run(spark)
    //Chapter4.ManageundUnmanagetables.run(spark)
     //Chapter4.Framesandtables.run(spark)
    //Chapter5.CubeFunction.run(spark)
    //Chapter5.AzureConnect.run(spark)
    // Run the JDBCConnect

    // === 3️⃣ Configuración JDBC para PostgreSQL ===
    //val pgUrl = "jdbc:postgresql://localhost:5432/clothes_dataset" // tu DB
    //val pgTable = "customers"                                   // tu tabla
    //val pgUser = "postgres"                                  // usuario
   // val pgPassword = "11@Carlos$"                              // contraseña

    // Ejecutar tu código JDBC
    //Chapter5.JDBCConnect.run(
     // spark,
     // pgUrl,
      //pgTable,
     // pgUser,
     // pgPassword
    //)

    // === 2️⃣ Parámetros de conexión a Cosmos DB ===
   // val accountEndpoint = "https://<YOUR_ACCOUNT>.documents.azure.com:443/"
   // val masterKey       = "<YOUR_MASTER_KEY>"
    //val database        = "<YOUR_DATABASE>"
   // val container       = "<YOUR_CONTAINER>"

    // Ejemplo de query (puedes adaptarla)
    //val query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"

    // === 3️⃣ Llamada a AzureConnect ===
   // Chapter5.AzureConnect.run(
     // spark,
    //  accountEndpoint,
     // masterKey,
     // database,
     // container,
     // query,
     // SaveMode.Overwrite // o SaveMode.Append si quieres agregar
    //)


    //Chapter5.TempArrays.run
    //Chapter6.Bloggers.run(spark)
    //Chapter6.UsageJob.run(spark)
    //Chapter6.TransformationDF_DT.run(spark)
    //Chapter7.SparkConfigJob.run()
    //Chapter7.CacheExampleJob.run()
    //Chapter7.JoinExampleJob.run()
    //Chapter7.BucketedJoinJob.run()
    Chapter9.DeltaLakesJob.run(spark)

    // Aquí puedes poner el resto de tu código usando `spark`
    // Por ejemplo, leer CSV, procesar DataFrame, etc.
    // Cerrar SparkSession al final
    spark.stop()
  }
}
