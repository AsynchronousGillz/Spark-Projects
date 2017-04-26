/*

val data = spark.read.format("libsvm").load("/MLData.txt")

import org.apache.spark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.{GBTRegressionModel, GBTRegressor}

val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(data)
val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2))
val gbt = new GBTRegressor().setLabelCol("label").setFeaturesCol("indexedFeatures").setMaxIter(10)
val pipeline = new Pipeline().setStages(Array(featureIndexer, gbt))
val model = pipeline.fit(trainingData)
val predictions = model.transform(testData)
predictions.select("prediction", "label").show(5)
predictions.select("prediction", "label").rdd.saveAsTextFile("/MLVal")
val evaluator = new RegressionEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)
println("Root Mean Squared Error (RMSE) on test data = " + rmse)

*/
