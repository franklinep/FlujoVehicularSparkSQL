import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties

object SparkConfig {

  // =========================
  // CONFIGURACIÓN GLOBAL
  // =========================
  // Ajusta estos valores UNA vez y se reutilizan en todos los Qx.scala
  val jdbcUrl: String = "jdbc:postgresql://localhost:5432/pc4"
  val dbUser:  String = "jhozzel"
  val dbPass:  String = "1234"

  // =========================
  // PROPIEDADES JDBC
  // =========================
  def jdbcProps(): Properties = {
    val props = new Properties()
    props.setProperty("user", dbUser)
    props.setProperty("password", dbPass)
    props.setProperty("driver", "org.postgresql.Driver")
    props
  }

  // =========================
  // CREAR SPARK SESSION
  // =========================
  def buildSpark(appName: String): SparkSession = {
    SparkSession.builder()
      .appName(appName)
      .master("local[*]")
      .getOrCreate()
  }

  // =========================
  // LEER TABLAS DESDE POSTGRES
  // =========================
  def readPeaje(spark: SparkSession): DataFrame =
    spark.read.jdbc(jdbcUrl, "peaje", jdbcProps())

  def readFlujo(spark: SparkSession): DataFrame =
    spark.read.jdbc(jdbcUrl, "flujo_peaje", jdbcProps())

  // =========================
  // HELPER DRY:
  // CREA SPARK, LEE TABLAS Y EJECUTA UN BLOQUE
  // =========================
  def withSparkAndDataFrames(appName: String)(
    f: (SparkSession, DataFrame, DataFrame) => Unit
  ): Unit = {
    val spark = buildSpark(appName)
    spark.sparkContext.setLogLevel("WARN")

    try {
      val peajeDf = readPeaje(spark)
      val flujoDf = readFlujo(spark)
      f(spark, peajeDf, flujoDf)
    } finally {
      spark.stop()
    }
  }
}
