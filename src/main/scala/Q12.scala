import org.apache.spark.sql.SparkSession

/*
  Objetivo:
    Calcular el promedio del total de vehículos por departamento en un año dado
    y ordenarlos de mayor a menor, combinando GROUP BY, AVG y ORDER BY.
*/

object Q12_SQL_OrderBy2 {
  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q12-SQL-OrderBy2") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        peajeDf.createOrReplaceTempView("peaje")
        flujoDf.createOrReplaceTempView("flujo_peaje")

        val query =
          """
            |SELECT f.anio,
            |       p.departamento,
            |       AVG(f.veh_total) AS promedio_veh_total
            |FROM flujo_peaje f
            |JOIN peaje p
            |  ON f.id_peaje = p.id_peaje
            |WHERE f.anio = 2023
            |GROUP BY f.anio, p.departamento
            |ORDER BY promedio_veh_total DESC
            |""".stripMargin

        val result = spark.sql(query)

        result.show(50, truncate = false)
    }
  }
}
