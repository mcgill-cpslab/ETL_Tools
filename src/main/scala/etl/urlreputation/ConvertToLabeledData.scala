package etl.urlreputation

import java.io.{File, BufferedReader, InputStreamReader}

import scala.collection.mutable.ListBuffer

import com.typesafe.config.ConfigFactory
import etl.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.ChiSqSelector
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD

object ConvertToLabeledData {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      // println("Usage: program rootPath outPath partitionNum")
      println("Usage: program configPath")
      sys.exit(-1)
    }
    val sc = new SparkContext()
    val conf = ConfigFactory.parseFile(new File(args(0)))
    val partitions = conf.getInt("etl.url.partitions")
    val inputDir = conf.getString("etl.url.inputPath")
    val outputDir = conf.getString("etl.url.outputPath")
    val featureSelection = conf.getBoolean("etl.url.featureSelection")
    val rootPath = new Path(inputDir)
    val svmData = MLUtils.loadLibSVMFile(sc, inputDir)

    val outputData = {
      if (featureSelection) {
        val featureSelectThreshold = conf.getInt("etl.url.featureNumber")
        val selector = new ChiSqSelector(featureSelectThreshold)
        val transformer = selector.fit(svmData.repartition(1000))
        svmData.map{lp => LabeledPoint(
          if (lp.label > 0) lp.label else 0,
          transformer.transform(lp.features))}
      } else {
        svmData.map{lp => LabeledPoint(if (lp.label > 0) lp.label else 0, lp.features)}
      }
    }.repartition(partitions)

    println(s"get ${outputData.count()} instances in total")
    outputData.saveAsTextFile(outputDir)
  }
}
