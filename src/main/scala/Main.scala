import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SparkSession, Dataset}

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

    val dll = "C:\\hadoop\\bin\\hadoop.dll"
    println("Loading: " + dll)
    System.load(dll)
    println("OK: hadoop.dll loaded")

    System.load(dll)
    println("Loaded: " + dll)
    println("Native loaded? " + org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded())
    // === 2️⃣ Crear SparkSession con Hive habilitado ===
    val spark: SparkSession = Spark.getSparkSession("SparkScalaApp", hive = true)
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
     Chapter4.Framesandtables.run(spark)


    // Aquí puedes poner el resto de tu código usando `spark`
    // Por ejemplo, leer CSV, procesar DataFrame, etc.
    // Cerrar SparkSession al final
    spark.stop()
  }
}
