package sparksqlexercises

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object GooglePlayAnalysis {

  def run(spark: SparkSession): Unit = {

    // 1. Cargar los ficheros
    val appsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/Datasets/googleplaystore.csv")

    val reviewsDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("src/main/resources/Datasets/googleplaystore_user_reviews.csv")

    // 2. Eliminar la app “Life Made WI-FI Touchscreen Photo Frame”
    val filteredDF = appsDF.filter(col("App") =!= "Life Made WI-FI Touchscreen Photo Frame")

    // 3. Sustituir valores NaN en Rating (promedio)
    val ratingMean = filteredDF.agg(avg("Rating")).first().getDouble(0)
    val ratingFilledDF = filteredDF.na.fill(Map("Rating" -> ratingMean))

    // 4. Sustituir NaN en Type por "Unknown"
    val typeFilledDF = ratingFilledDF.na.fill(Map("Type" -> "Unknown"))

    // 5. Agregar columna que indica si las características varían según el dispositivo
    val variationDF = typeFilledDF.withColumn(
      "Varies_by_Device",
      when(col("Size").isNotNull && col("Android Ver").isNotNull, "Yes").otherwise("No")
    )

    // 6. Crear nueva columna Frec_Download según número de instalaciones
    val cleanedDF = variationDF.withColumn(
      "Installs_Num",
      regexp_replace(col("Installs"), "[+,]", "").cast("long")
    )

    val df_limpio = cleanedDF.withColumn(
      "Frec_Download",
      when(col("Installs_Num") < 50000, "Baja")
        .when(col("Installs_Num") >= 50000 && col("Installs_Num") < 1000000, "Media")
        .when(col("Installs_Num") >= 1000000 && col("Installs_Num") < 50000000, "Alta")
        .otherwise("Muy alta")
    )

    // --- Mostrar resultado final ---
    df_limpio.show(10, truncate = false)

    // --- Consultas adicionales ---

    // a. Aplicaciones con Frec_Download "Muy alta" y Rating > 4.5
    println("Aplicaciones con Frec_Download 'Muy alta' y Rating > 4.5:")
    df_limpio.filter(col("Frec_Download") === "Muy alta" && col("Rating") > 4.5)
      .show(10, truncate = false)

    // b. Número de aplicaciones con Frec_Download "Muy alta" y Type "Free"
    val countHighFree = df_limpio.filter(col("Frec_Download") === "Muy alta" && col("Type") === "Free")
      .count()
    println(s"Número de aplicaciones con Frec_Download 'Muy alta' y coste gratuito: $countHighFree")

    // c. Aplicaciones con Price < 13
    println("Aplicaciones con Price < 13 dólares:")
    df_limpio.filter(col("Price") < 13).show(10, truncate = false)

    // --- Trabajar con sample para pruebas ---
    val sampleDF = df_limpio.sample(withReplacement = false, fraction = 0.1, seed = 123)
    println("Muestra del 10% del dataset para pruebas:")
    sampleDF.show(10, truncate = false)
  }
}


