import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Objetivo:
    Mostrar, por año, cuántos peajes distintos hay por tipo de administración,
    en formato wide (una columna por tipo de administración).
*/

object Q4_PivotPeajesPorAnio_Robusto {

  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q4-PivotPeajesPorAnio-Robusto") {
      (spark: SparkSession, peajeDf, flujoDf) =>
        import spark.implicits._

        val joined = flujoDf.join(peajeDf, "id_peaje")

        val porAnioYAdmin = joined
          .groupBy($"anio", $"administracion")
          .agg(countDistinct($"id_peaje").as("num_peajes"))

        val pivoted = porAnioYAdmin
          .groupBy($"anio")
          .pivot($"administracion")
          .agg(first($"num_peajes"))
          .na.fill(0)

        println("=== Peajes por año y tipo de administracion (pivot dinámico) ===")
        pivoted.orderBy($"anio").show(100, truncate = false)
    }
  }
}
