import org.apache.spark.sql.SparkSession

/*
  Objetivo:
    Usar vistas temporales y SQL para realizar una selección que
    combine datos de flujo_peaje y peaje mediante un JOIN.
*/

object Q8_SQL_Join {
  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q8-SQL-Join") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        peajeDf.createOrReplaceTempView("peaje")
        flujoDf.createOrReplaceTempView("flujo_peaje")

        val query =
          """
            |SELECT f.anio,
            |       f.mes,
            |       p.nombre_peaje,
            |       p.departamento,
            |       f.veh_total
            |FROM flujo_peaje f
            |JOIN peaje p
            |  ON f.id_peaje = p.id_peaje
            |WHERE f.anio = 2020
            |ORDER BY f.mes, p.nombre_peaje
            |""".stripMargin

        val result = spark.sql(query)

        result.show(50, truncate = false)
    }
  }
}
