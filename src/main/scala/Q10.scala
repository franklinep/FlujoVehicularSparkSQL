import org.apache.spark.sql.SparkSession

/*
  Objetivo:
    Contar el número de peajes distintos por tipo de administración
    y año, usando SQL con GROUP BY y COUNT(DISTINCT).
*/

object Q10_SQL_GroupByCount2 {
  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q10-SQL-GroupByCount2") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        peajeDf.createOrReplaceTempView("peaje")
        flujoDf.createOrReplaceTempView("flujo_peaje")

        val query =
          """
            |SELECT f.anio,
            |       p.administracion,
            |       COUNT(DISTINCT f.id_peaje) AS num_peajes
            |FROM flujo_peaje f
            |JOIN peaje p
            |  ON f.id_peaje = p.id_peaje
            |GROUP BY f.anio, p.administracion
            |ORDER BY f.anio, p.administracion
            |""".stripMargin

        val result = spark.sql(query)

        result.show(100, truncate = false)
    }
  }
}
