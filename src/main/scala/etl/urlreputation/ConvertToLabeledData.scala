package etl.urlreputation

import java.io.{BufferedReader, InputStreamReader}

import scala.collection.mutable.ListBuffer

import etl.Utils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object ConvertToLabeledData {

  private def fromLineToLabeledPoint(vectorSize: Int, line: String): LabeledPoint = {
    try {
      val completeArray = line.split(" ")
      val featureArray = completeArray.tail
      val labeledPointArray = featureArray.map(_.split(":")).map(array => (array(0), array(1)))
      val indexArray = labeledPointArray.map(_._1.toInt)
      val valueArray = labeledPointArray.map(_._2.toDouble)
      LabeledPoint(completeArray(0).toDouble, new SparseVector(vectorSize, indexArray, valueArray))
    } catch {
      case e: Exception =>
        e.printStackTrace()
        println(line)
        null
    }
  }

  private def loadDataAndConvertToLabeledData(
     sc: SparkContext,
     allFilesPath: ListBuffer[String],
     outputPartitionNum: Int,
     vectorSize: Int): RDD[LabeledPoint] = {
    val allFilesPathRDD = sc.parallelize(allFilesPath, outputPartitionNum)
    val labeledPointsRDD = allFilesPathRDD.flatMap(sourcePathString =>  {
      val hadoopConf = new Configuration()
      val sourcePath = new Path(sourcePathString)
      val sourceFs = sourcePath.getFileSystem(hadoopConf)
      val fileHandler = sourceFs.open(sourcePath)
      val isr = new InputStreamReader(fileHandler)
      val br = new BufferedReader(isr)
      var line = br.readLine()
      val pointsArray = new ListBuffer[LabeledPoint]
      while (line != null && line.length > 1) {
        pointsArray += fromLineToLabeledPoint(vectorSize, line)
        line = br.readLine()
      }
      fileHandler.close()
      isr.close()
      br.close()
      pointsArray
    })
    labeledPointsRDD
  }

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println("Usage: program rootPath outPath partitionNum")
      sys.exit(-1)
    }
    val sc = new SparkContext()
    val rootPath = new Path(args(0))
    val allFilesToProcess = new ListBuffer[String]
    Utils.getAllFilePath(rootPath.getFileSystem(sc.hadoopConfiguration),
      rootPath, allFilesToProcess)
    val labeledPoints = loadDataAndConvertToLabeledData(
      sc,
      vectorSize = 3231961,
      allFilesPath = allFilesToProcess,
      outputPartitionNum = args(2).toInt)
    println(s"get ${labeledPoints.count()} instances in total")
    labeledPoints.saveAsTextFile(args(1))
  }
}
