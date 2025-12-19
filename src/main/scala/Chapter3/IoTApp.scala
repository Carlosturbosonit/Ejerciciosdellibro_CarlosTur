package Chapter3

import org.apache.spark.sql.{Dataset, SparkSession}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

// Case class principal
case class DeviceIoTData(
                          battery_level: Long,
                          c02_level: Long,
                          cca2: String,
                          cca3: String,
                          cn: String,
                          device_id: Long,
                          device_name: String,
                          humidity: Long,
                          ip: String,
                          latitude: Double,
                          lcd: String,
                          longitude: Double,
                          scale: String,
                          temp: Long,
                          timestamp: Long
                        )

// Nueva case class para la proyección
case class DeviceTempByCountry(temp: Long, device_name: String, device_id: Long, cca3: String)

object IoTApp {

  def run(spark: SparkSession): Unit = {
    import spark.implicits._  // 🔹 necesario para map, as, toDF

    val jsonPath = generateJSON()
    val ds = readJSON(spark, jsonPath)

    println("=== Dataset completo ===")
    ds.show(false)

    // Filtro de temp > 20 y humedad > 40
    val filterTempDS = ds.filter(d => d.temp > 20 && d.humidity > 40)
    println("=== Dispositivos con temp > 20 y humedad > 40 ===")
    filterTempDS.show(false)

    // Proyección a DeviceTempByCountry filtrando temp > 25
    val dsTemp = ds
      .filter(d => d.temp > 25)
      .map(d => DeviceTempByCountry(d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry] // convertimos de nuevo a Dataset

    println("=== Proyección DeviceTempByCountry con temp > 25 ===")
    dsTemp.show(false)

    // Tomar el primer dispositivo
    val device = dsTemp.first()
    println("=== Primer dispositivo filtrado ===")
    println(device)

    // Alternativa: usando select y where
    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .where($"temp" > 25)
      .as[DeviceTempByCountry]

    println("=== dsTemp2 (selección por columnas y filtro) ===")
    dsTemp2.show(false)
  }

  // Función que genera el JSON
  private def generateJSON(): String = {
    val data = Seq(
      """{"device_id":198164,"device_name":"sensor-pad-198164owomcJZ","ip":"80.55.20.25","cca2":"PL","cca3":"POL","cn":"Poland","latitude":53.08,"longitude":18.62,"scale":"Celsius","temp":21,"humidity":65,"battery_level":8,"c02_level":1408,"lcd":"red","timestamp":1458081226051}""",
      """{"device_id":198165,"device_name":"sensor-pad-198165owomcJZ","ip":"80.55.20.26","cca2":"DE","cca3":"DEU","cn":"Germany","latitude":51.17,"longitude":10.45,"scale":"Celsius","temp":34,"humidity":75,"battery_level":9,"c02_level":1200,"lcd":"green","timestamp":1458081226052}"""
    )

    val path = "C:/temp/iot_devices.json"
    Files.write(Paths.get(path), data.mkString("\n").getBytes(StandardCharsets.UTF_8))
    println(s"Archivo JSON creado en: $path")
    path
  }

  // Función que lee el JSON
  private def readJSON(spark: SparkSession, jsonPath: String): Dataset[DeviceIoTData] = {
    import spark.implicits._
    spark.read.json(jsonPath).as[DeviceIoTData]
  }
}
