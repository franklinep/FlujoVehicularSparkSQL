import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Objetivo:
    Calcular el promedio del índice medio diario de vehículos (veh_imd)
    por año, para entender la evolución del flujo promedio en el tiempo.

  Consulta:
    - Usar la tabla flujo_peaje.
    - Agrupar por 'anio'.
    - Calcular AVG(veh_imd) como 'promedio_veh_imd'.
    - Ordenar por 'anio'.

  Output esperado:
    Tabla con columnas:
      anio | promedio_veh_imd
*/

object Q5_PromedioVehImdPorAnio {

  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q5-PromedioVehImdPorAnio") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        val resultado = flujoDf
          .groupBy("anio")
          .agg(avg("veh_imd").as("promedio_veh_imd"))
          .orderBy("anio")

        println("=== Promedio de veh_imd por año ===")
        resultado.show(50, truncate = false)
    }
  }
}
