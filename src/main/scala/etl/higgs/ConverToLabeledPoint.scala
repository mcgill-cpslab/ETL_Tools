package etl.higgs

import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint

/**
 * https://archive.ics.uci.edu/ml/datasets/HIGGS
 */
object ConverToLabeledPoint {
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
        val label = featureLabelArray(0).toDouble
        val features = featureLabelArray.tail
        val nonZeroIndices = features.indices.filter(i => features(i).toDouble != 0)
        val nonZeroValues = features.map(_.toDouble).filter(_ != 0)
        LabeledPoint(label,
          new SparseVector(featureLabelArray.length - 1, nonZeroIndices.toArray, nonZeroValues))
    }.repartition(args(2).toInt)
    labeledPoints.saveAsTextFile(args(1))
  }
}
