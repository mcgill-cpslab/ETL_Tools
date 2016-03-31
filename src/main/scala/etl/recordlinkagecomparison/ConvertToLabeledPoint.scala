package etl.recordlinkagecomparison

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

// dataset URL: https://archive.ics.uci.edu/ml/datasets/Record+Linkage+Comparison+Patterns
object ConvertToLabeledPoint {
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("FAULT: program inputPath outputPath partitionNum")
    }
    val sc = new SparkContext()
    val inputPath = args(0)
    val strRDD = sc.textFile(inputPath)
    val labeledPoints = strRDD.map{
      line =>
        val featureLabelArray = line.split(",")
        val label = if (featureLabelArray(featureLabelArray.length - 1) == "TRUE") 1 else 0
        val feature = featureLabelArray.take(featureLabelArray.length - 1).map(featureStr =>
          if (featureStr == "?") {
            0.0
          } else {
            featureStr.toDouble
          }
        )
        val nonZeroIndices = feature.indices.filter(i => feature(i) != 0).toArray
        val nonZeroValues = feature.filter(_ != 0)
        val sparseFeature = new SparseVector(11, nonZeroIndices, nonZeroValues)
        (label, sparseFeature)
    }.repartition(args(2).toInt).cache()

    val labels = labeledPoints.map(_._1)

    val features = labeledPoints.map(_._2).map(Tuple1.apply)

    val polynomialExpansion = new PolynomialExpansion()
    val sqlContext = new SQLContext(sc)
    val df = sqlContext.createDataFrame(features).toDF("features")

    polynomialExpansion.setInputCol("features").setOutputCol("polyFeatures").setDegree(3)
    val expandedFeature = polynomialExpansion.transform(df)
    val featureRDD = expandedFeature.select("polyFeatures").rdd.map(row =>
      row.getAs[SparseVector](0))
    labels.zip(featureRDD).map{case (label, features) => LabeledPoint(label, features)}.saveAsTextFile(args(1))
  }
}
