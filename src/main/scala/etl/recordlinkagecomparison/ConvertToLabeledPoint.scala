package etl.recordlinkagecomparison

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{DenseVector, SparseVector}
import org.apache.spark.mllib.regression.LabeledPoint

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
        new LabeledPoint(label, sparseFeature)
    }.repartition(args(2).toInt)
    labeledPoints.saveAsTextFile(args(1))
  }
}
