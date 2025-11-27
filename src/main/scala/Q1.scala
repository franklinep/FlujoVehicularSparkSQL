import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Objetivo:
    Mostrar columnas específicas de las tablas peaje y flujo_peaje
    para tener una vista rápida de los datos relevantes.
*/

object Q1_MostrarColumnas {

  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q1-MostrarColumnas") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        peajeDf
          .select("nombre_peaje", "departamento")
          .show(10, truncate = false)

        flujoDf
          .select("anio", "mes", "veh_total", "veh_imd")
          .show(10, truncate = false)
    }
  }
}
