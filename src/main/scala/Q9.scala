import org.apache.spark.sql.SparkSession

/*
  Objetivo:
    Contar cuántos registros de flujo hay por año usando SQL
    y vistas temporales.
*/

object Q9_SQL_GroupByCount1 {
  def main(args: Array[String]): Unit = {
    SparkConfig.withSparkAndDataFrames("Q9-SQL-GroupByCount1") {
      (spark: SparkSession, peajeDf, flujoDf) =>

        flujoDf.createOrReplaceTempView("flujo_peaje")

        val query =
          """
            |SELECT anio,
            |       COUNT(*) AS registros
            |FROM flujo_peaje
            |GROUP BY anio
            |ORDER BY anio
            |""".stripMargin

        val result = spark.sql(query)

        result.show(50, truncate = false)
    }
  }
}
