import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.classification.{LogisticRegression, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.Pipeline
import java.util.Properties

object ModelFlujoVehicularML {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("FlujoVehicular-ML")
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

    // Creamos un label que nos indicaron el nivel de flujo vehicular
    //    label = 1.0  --> mes de alto flujo
    //    label = 0.0  --> mes de bajo flujo
    val mediana = base.stat.approxQuantile("veh_total", Array(0.5), 0)(0)

    val labeled = base.withColumn(
      "label",
      when(col("veh_total") >= mediana, 1.0).otherwise(0.0)
    )

    println("=== LABEL DISTRIBUTION ORIGINAL ===")
    labeled.groupBy("label").count().show()

    // Balanceo de clases
    val minority = labeled.filter($"label" === 1.0)
    val majority = labeled.filter($"label" === 0.0)

    val c0 = majority.count()
    val c1 = minority.count()

    val ratio = Math.max(1, (c0 / c1).toInt)

    val minorityOversampled =
      (1 to ratio).map(_ => minority).reduce(_ union _)

    val balanced = majority.union(minorityOversampled)

    println("=== DATASET BALANCEADO ===")
    balanced.groupBy("label").count().show()

    // Se convierte texto en números, codifica categorías y agrupa todas las columnas en el vector features que Spark ML necesita para entrenar los modelos.
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

    val feModel = pipeline.fit(balanced)
    val data = feModel.transform(balanced)

    // train/ test split
    val Array(train, test) = data.randomSplit(Array(0.8, 0.2), 1234L)

    // MODELO 01: Logistic Regression
    val lr = new LogisticRegression()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxIter(50)

    val lrModel = lr.fit(train)
    val predLR = lrModel.transform(test)

    println("=== PREDICCIONES LR (EJEMPLOS) ===")
    predLR.select("anio","mes","departamento","veh_total","label","prediction","probability")
      .show(10, truncate = false)

    // Matriz de confusion
    println("=== MATRIZ DE CONFUSIÓN LR ===")
    predLR.groupBy("label", "prediction")
      .count()
      .orderBy("label", "prediction")
      .show()

    // MODELO 02: Multilayer Perceptron
    val numFeatures = train.head().getAs[Vector]("features").size
    val layers = Array(numFeatures, 8, 4, 2) // input, 2 ocultas, salida (2 clases)

    val mlp = new MultilayerPerceptronClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setLayers(layers)
      .setMaxIter(100)

    val mlpModel = mlp.fit(train)
    val predMLP = mlpModel.transform(test)

    println("=== PREDICCIONES MLP (EJEMPLOS) ===")
    predMLP.select("anio","mes","departamento","veh_total","label","prediction","probability")
      .show(10, truncate = false)

    // Matriz de confusion
    println("=== MATRIZ DE CONFUSIÓN MLP ===")
    predMLP.groupBy("label", "prediction")
      .count()
      .orderBy("label", "prediction")
      .show()

    // Realizamos las metricas que necesitamos
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

    val accLR  = accEval.evaluate(predLR)
    val recLR  = recEval.evaluate(predLR)
    val f1LR   = f1Eval.evaluate(predLR)
    val lossLR = computeLogLoss(predLR)

    val accMLP  = accEval.evaluate(predMLP)
    val recMLP  = recEval.evaluate(predMLP)
    val f1MLP   = f1Eval.evaluate(predMLP)
    val lossMLP = computeLogLoss(predMLP)

    val metrics = Seq(
      ("Regresión logística",    accLR,  recLR,  f1LR,  lossLR),
      ("Multilayer Perceptron",  accMLP, recMLP, f1MLP, lossMLP)
    ).toDF("modelo", "accuracy", "recall", "f1_score", "loss")

    println("=== TABLA FINAL DE MÉTRICAS ===")
    metrics.show(truncate = false)

    spark.stop()
  }

  // Calculamos el log-loss
  def computeLogLoss(pred: DataFrame): Double = {
    val eps = 1e-15

    val rdd = pred.select("label", "probability").rdd.map { row =>
      // label: 0.0 o 1.0
      val y   = row.getAs[Double]("label")
      // probability: Vector (Dense o Sparse)
      val vec = row.getAs[Vector]("probability")

      // prob de la clase 1
      val p1 =
        if (vec != null && vec.size > 1) vec(1)
        else 0.5   // valor neutro en caso raro

      // probabilidad de la clase verdadera
      val pTrue = if (y == 1.0) p1 else 1.0 - p1

      val pClipped = math.max(eps, math.min(1.0 - eps, pTrue))

      // -[ y log(p) + (1-y) log(1-p) ]
      -( y * math.log(pClipped) + (1.0 - y) * math.log(1.0 - pClipped) )
    }

    rdd.mean()
  }
}
