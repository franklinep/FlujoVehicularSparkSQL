import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object LoadToPostgres {

  def main(args: Array[String]): Unit = {

    // CONFIGURACIÓN LOCAL

    val csvPath = "C:\\Users\\frank\\IdeaProjects\\FlujoVehicularPeaje\\src\\main\\flujo_vehicular_peajes_2014_2025.csv"
    val jdbcUrl = "jdbc:postgresql://localhost:5432/dbflujovehicular"
    val dbUser  = "postgres"
    val dbPass  = "postgres"

    val spark = SparkSession.builder()
      .appName("FlujoVehicularPeaje-LoadToPostgres")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val connectionProps = new Properties()
    connectionProps.setProperty("user", dbUser)
    connectionProps.setProperty("password", dbPass)
    connectionProps.setProperty("driver", "org.postgresql.Driver")

    // 1. LEER CSV

    val rawDf = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "UTF-8")
      .csv(csvPath)

    println("===== ESQUEMA DEL CSV =====")
    rawDf.printSchema()

    // 2. TABLA PEAJE (EVITAR DUPLICADOS)

    val peajeDf = rawDf
      .select(
        col("NOMBRE_PEAJE").as("nombre_peaje"),
        col("DEPARTAMENTO").as("departamento"),
        col("ADMINIST").as("administracion")
      )
      .distinct()

    val existingPeajesBase =
      spark.read
        .jdbc(jdbcUrl, "peaje", connectionProps)
        .select("nombre_peaje", "departamento")
        .distinct()

    val peajesNuevos = peajeDf.as("p")
      .join(
        existingPeajesBase.as("e"),
        Seq("nombre_peaje", "departamento"),
        "left_anti"
      )

    println(s"Peajes nuevos a insertar: ${peajesNuevos.count()}")

    peajesNuevos.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "peaje", connectionProps)

    println("===== TABLA 'peaje' ACTUALIZADA =====")


    // 3. LEER TABLA PEAJE CON id_peaje

    val peajeDbDf = spark.read
      .jdbc(jdbcUrl, "peaje", connectionProps)
      .select("id_peaje", "nombre_peaje", "departamento", "administracion")
      .distinct()

    // 4. PREPARAR DF PARA flujo_peaje

    val flujoEnriquecido = rawDf
      .join(
        peajeDbDf,
        rawDf("NOMBRE_PEAJE") === peajeDbDf("nombre_peaje") &&
          rawDf("DEPARTAMENTO") === peajeDbDf("departamento") &&
          rawDf("ADMINIST") === peajeDbDf("administracion"),
        "inner"
      )

    val flujoFinalDf = flujoEnriquecido.select(
      col("ID").cast("bigint").as("id_flujo"),
      col("id_peaje"),

      col("ANIO").cast("int").as("anio"),
      col("MES").cast("int").as("mes"),

      col("VEH_LIGEROS_TAR_DIF").cast("int").as("veh_ligeros_tar_dif"),
      col("VEH_LIGEROS_AUTOMOVILES").cast("double").cast("int")
        .as("veh_ligeros_automoviles"),

      col("VEH_LIGEROS_TOTAL").cast("int").as("veh_ligeros_total"),
      col("VEH_LIGEROS_IMD").cast("double").as("veh_ligeros_imd"),

      col("VEH_PESADOS_TAR_DIF").cast("int").as("veh_pesados_tar_dif"),
      col("VEH_PESADOS__2E").cast("double").cast("int").as("veh_pesados_2e"),
      col("VEH_PESADOS_3E").cast("double").cast("int").as("veh_pesados_3e"),
      col("VEH_PESADOS_4E").cast("double").cast("int").as("veh_pesados_4e"),
      col("VEH_PESADOS_5E").cast("double").cast("int").as("veh_pesados_5e"),
      col("VEH_PESADOS_6E").cast("double").cast("int").as("veh_pesados_6e"),
      col("VEH_PESADOS_7E").cast("double").cast("int").as("veh_pesados_7e"),

      col("VEH_PESADOS_TOTAL").cast("double").cast("int")
        .as("veh_pesados_total"),
      col("VEH_PESADOS_IMD").cast("double").as("veh_pesados_imd"),

      col("VEH_TOTAL").cast("double").cast("int").as("veh_total"),
      col("VEH_IMD").cast("double").as("veh_imd")
    ).dropDuplicates("id_flujo")

    println("===== EJEMPLO DE 'flujo_peaje' =====")
    flujoFinalDf.show(10, truncate = false)

    flujoFinalDf.write
      .mode(SaveMode.Append)
      .jdbc(jdbcUrl, "flujo_peaje", connectionProps)

    println("===== TABLA 'flujo_peaje' CARGADA =====")

    spark.stop()
  }
}
