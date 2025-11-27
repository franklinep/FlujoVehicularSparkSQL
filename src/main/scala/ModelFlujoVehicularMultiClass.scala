import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.{
  LogisticRegression,
  DecisionTreeClassifier,
  RandomForestClassifier
}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline
import java.util.Properties

object ModelFlujoVehicularMultiClass {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FlujoVehicular-MultiClass")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val jdbcUrl = "jdbc:postgresql://localhost:5432/pc4"
    val dbUser  = "jhozzel"
    val dbPass  = "1234"

    val props = new Properties()
    props.setProperty("user", dbUser)
    props.setProperty("password", dbPass)
    props.setProperty("driver", "org.postgresql.Driver")

    // Leemos los datos de postgres
    val flujoDf = spark.read.jdbc(jdbcUrl, "flujo_peaje", props)
    val peajeDf = spark.read.jdbc(jdbcUrl, "peaje", props)

    val base = flujoDf
      .join(peajeDf, "id_peaje")
      .select(
        col("anio").cast("double"),
        col("mes").cast("double"),
        col("veh_ligeros_imd").cast("double"),
        col("veh_pesados_imd").cast("double"),
        col("veh_total").cast("double"),
        col("departamento"),
        col("administracion")
      )
      .na.fill(0)

    println("=== SAMPLE BASE ===")
    base.show(10, truncate = false)

    // Creamos un label para multiclase (0 = bajo, 1 = medio, 2 = alto)
    val Array(q33, q66) = base.stat.approxQuantile("veh_total", Array(0.33, 0.66), 0)

    val labeled = base.withColumn(
      "label",
      when(col("veh_total") < q33, 0.0)
        .when(col("veh_total") < q66, 1.0)
        .otherwise(2.0)
    )

    println("=== DISTRIBUCIÓN DE CLASES (0=bajo,1=medio,2=alto) ===")
    labeled.groupBy("label").count().show()


    val deptIdx = new StringIndexer()
      .setInputCol("departamento")
      .setOutputCol("departamento_idx")
      .setHandleInvalid("keep")

    val admIdx = new StringIndexer()
      .setInputCol("administracion")
      .setOutputCol("administracion_idx")
      .setHandleInvalid("keep")

    val encoder = new OneHotEncoder()
      .setInputCols(Array("departamento_idx", "administracion_idx"))
      .setOutputCols(Array("departamento_vec", "administracion_vec"))

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "anio", "mes", "veh_ligeros_imd", "veh_pesados_imd",
        "departamento_vec", "administracion_vec"
      ))
      .setOutputCol("features")

    val pipeline = new Pipeline()
      .setStages(Array(deptIdx, admIdx, encoder, assembler))

    val feModel = pipeline.fit(labeled)
    val data = feModel.transform(labeled)

    // Entrenamos el modelo, test split
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), 1234L)

    // MODELO 01: Arbol de decision
    val dt = new DecisionTreeClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxDepth(6)

    val dtModel = dt.fit(train)
    val predDT = dtModel.transform(test)

    println("=== PREDICCIONES ÁRBOL DE DECISIÓN (ejemplos) ===")
    predDT.select("anio","mes","departamento","veh_total","label","prediction","probability")
      .show(10, truncate = false)

    // MODELO 02: Random Forest
    val rf = new RandomForestClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setNumTrees(50)
      .setMaxDepth(8)

    val rfModel = rf.fit(train)
    val predRF = rfModel.transform(test)

    println("=== PREDICCIONES RANDOM FOREST (ejemplos) ===")
    predRF.select("anio","mes","departamento","veh_total","label","prediction","probability")
      .show(10, truncate = false)

    // MODELO 03: REGRESIÓN LOGÍSTICA MULTICLASE
    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(80)
      .setFamily("multinomial")   // multinomial para multiclase

    val lrModel = lr.fit(train)
    val predLR = lrModel.transform(test)

    println("=== PREDICCIONES REG. LOGÍSTICA MULTICLASE (ejemplos) ===")
    predLR.select("anio","mes","departamento","veh_total","label","prediction","probability")
      .show(10, truncate = false)

    // Imprimimos la matriz de confusion
    println("=== MATRIZ DE CONFUSIÓN - ÁRBOL DE DECISIÓN ===")
    predDT.groupBy("label","prediction").count()
      .orderBy("label","prediction")
      .show()

    println("=== MATRIZ DE CONFUSIÓN - RANDOM FOREST ===")
    predRF.groupBy("label","prediction").count()
      .orderBy("label","prediction")
      .show()

    println("=== MATRIZ DE CONFUSIÓN - REG. LOG. MULTICLASE ===")
    predLR.groupBy("label","prediction").count()
      .orderBy("label","prediction")
      .show()

    // Metricas: accuracy, recall, f1, pérdida
    val accEval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")

    val recEval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")

    val f1Eval = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")

    // decision tree
    val accDT  = accEval.evaluate(predDT)
    val recDT  = recEval.evaluate(predDT)
    val f1DT   = f1Eval.evaluate(predDT)
    val lossDT = computeLogLossMulticlass(predDT)

    // random forest
    val accRF  = accEval.evaluate(predRF)
    val recRF  = recEval.evaluate(predRF)
    val f1RF   = f1Eval.evaluate(predRF)
    val lossRF = computeLogLossMulticlass(predRF)

    // Logistic regression multiclase
    val accLR  = accEval.evaluate(predLR)
    val recLR  = recEval.evaluate(predLR)
    val f1LR   = f1Eval.evaluate(predLR)
    val lossLR = computeLogLossMulticlass(predLR)

    val metrics = Seq(
      ("Árbol de decisión", accDT, recDT, f1DT, lossDT),
      ("Random Forest",     accRF, recRF, f1RF, lossRF),
      ("Reg. Log. Multic.", accLR, recLR, f1LR, lossLR)
    ).toDF("modelo", "accuracy", "recall", "f1_score", "loss")

    println("=== TABLA DE MÉTRICAS (MULTICLASE) ===")
    metrics.show(truncate = false)

    spark.stop()
  }

  // Calculo del log-loss para MULTICLASE
  def computeLogLossMulticlass(pred: DataFrame): Double = {
    val eps = 1e-15

    val rdd = pred.select("label", "probability").rdd.map { row =>
      val y   = row.getAs[Double]("label")
      val vec = row.getAs[Vector]("probability")

      val yi = math.round(y).toInt
      val k  = if (vec != null) vec.size else 1

      val pTrue =
        if (vec != null && yi >= 0 && yi < k) vec(yi)
        else 1.0 / k

      val pClipped = math.max(eps, math.min(1.0 - eps, pTrue))

      // log-loss multiclase: -log(p(clase verdadera))
      -math.log(pClipped)
    }

    rdd.mean()
  }
}
