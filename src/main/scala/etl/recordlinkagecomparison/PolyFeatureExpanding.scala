package etl.recordlinkagecomparison

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.PolynomialExpansion
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext

object PolyFeatureExpanding {

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val labeledPoints = MLUtils.loadLabeledPoints(sc, args(0))

    val labels = labeledPoints.map(_.label)

    val features = labeledPoints.map(_.features).map(Tuple1.apply)

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
