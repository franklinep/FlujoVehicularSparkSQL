import org.apache.spark.sql.SparkSession

/*
  Objetivo:
    Listar para un año específico los peajes con mayor flujo total de vehículos,
    ordenados de mayor a menor.
*/

object Q11_SQL_OrderBy1 {
  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q11-SQL-OrderBy1") {
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
            |WHERE f.anio = 2019
            |ORDER BY f.veh_total DESC
            |""".stripMargin

        val result = spark.sql(query)

        result.show(50, truncate = false)
    }
  }
}
