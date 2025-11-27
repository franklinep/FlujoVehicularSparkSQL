import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Objetivo:
    Calcular, por peaje y año, el porcentaje de vehículos ligeros
    sobre el total de vehículos
*/

object Q7_PorcentajeLigeros {

  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q7-PorcentajeLigeros") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        import spark.implicits._

        val joined = flujoDf.join(peajeDf, "id_peaje")

        val agregados = joined
          .groupBy($"anio", $"nombre_peaje", $"departamento")
          .agg(
            sum($"veh_ligeros_total").as("total_ligeros"),
            sum($"veh_total").as("total_vehiculos")
          )

        val resultado = agregados
          .withColumn(
            "pct_ligeros",
            when(col("total_vehiculos") > 0,
              round(col("total_ligeros") / col("total_vehiculos") * 100.0, 2)
            ).otherwise(lit(0.0))
          )
          .orderBy(desc("pct_ligeros"))

        resultado.show(50, truncate = false)
    }
  }
}
