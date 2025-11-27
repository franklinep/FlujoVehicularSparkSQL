import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
  Objetivo:
    Ordenar los peajes por la cantidad total de vehículos en un año concreto,
    para identificar cuáles son los peajes con mayor flujo.
*/

object Q3_OrderBy {

  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q3-OrderBy") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        import spark.implicits._

        val joined = flujoDf.join(peajeDf, "id_peaje")

        val topPeajes2024 = joined
          .filter($"anio" === 2024)
          .groupBy("nombre_peaje", "departamento")
          .agg(sum("veh_total").as("vehiculos_2024"))
          .orderBy(desc("vehiculos_2024"))

        topPeajes2024.show()
    }
  }
}
