import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Objetivo:
    Filtrar los registros de flujo_peaje para ver solo aquellos
    con alto flujo vehicular en un año específico.

  Consulta:
    - Seleccionar registros donde:
        anio = 2019
        veh_total > 50000
*/

object Q2_Filter {

  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q2-Filter") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        import spark.implicits._

        val filtrado = flujoDf
          .filter($"anio" === 2019 && $"veh_total" > 50000)

        println("=== Registros con veh_total > 50000 en 2019 ===")
        filtrado
          .select("id_flujo", "anio", "mes", "veh_total")
          .show(50, truncate = false)
    }
  }
}
