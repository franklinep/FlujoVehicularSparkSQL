import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Objetivo:
    Unir la información de flujo (flujo_peaje) con el maestro de peajes (peaje)
    para ver, por peaje y año, el total de vehículos.
*/

object Q6_JoinFlujoPeaje {

  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q6-JoinFlujoPeaje") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        val joined = flujoDf.join(peajeDf, "id_peaje")

        val resultado = joined
          .select(
            col("anio"),
            col("mes"),
            col("nombre_peaje"),
            col("departamento"),
            col("veh_total")
          )
          .orderBy(col("anio"), col("mes"), col("nombre_peaje"))

        resultado.show(50, truncate = false)
    }
  }
}
