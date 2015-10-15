package etl.corel

import java.nio.charset.StandardCharsets
import java.nio.file.{StandardOpenOption, Paths, Files}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

import etl.vector.SparseVector

object CorelImageFeatures {

  val vectorDim = 32

  def loadAsSparseVector(inputPath: String): Array[SparseVector] = {
    val vectorArray = new ListBuffer[SparseVector]
    for (line <- Source.fromFile(inputPath).getLines()) {
      val weightsArray = line.split(" ")
      val vectorId = weightsArray(0).toInt
      val values = weightsArray.tail.map(_.toDouble)
      val interestedIndices = values.zipWithIndex.filter(_._1 != 0.0).map(_._2)
      val nonZeroValues = values.zipWithIndex.filter(x => interestedIndices.contains(x._2)).map(_._1)
      vectorArray += new SparseVector(vectorId, vectorDim, interestedIndices, values)
    }
    vectorArray.toArray
  }

  def separateIntoTrainingAndTestSet(allVectors: Array[SparseVector]):
    (Array[SparseVector], Array[SparseVector]) = {
    val testVectorIds = new mutable.HashSet[Int]
    while (testVectorIds.size < 50) {
      testVectorIds += Random.nextInt(allVectors.length)
    }
    val trainingSet = allVectors.zipWithIndex.filter(a => !testVectorIds.contains(a._2)).map(_._1)
    val testSet = allVectors.zipWithIndex.filter(a => testVectorIds.contains(a._2)).map(_._1)
    (trainingSet, testSet)
  }

  def outputFile(trainingSet: Array[SparseVector], testSet: Array[SparseVector], outputPath: String): Unit = {
    val size = vectorDim
    var cnt = 0

    trainingSet.foreach(vector => {
      cnt += 1
      Files.write(Paths.get(outputPath + "_training"), (vector.toString + "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND)
    })

    testSet.foreach(vector => {
      cnt += 1
      Files.write(Paths.get(outputPath + "_test"), (vector.toString + "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND)
    })
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: program input_path output_path")
      sys.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)
    val vectors = loadAsSparseVector(inputPath)
    val (trainingSet, testSet) = separateIntoTrainingAndTestSet(vectors)
    outputFile(trainingSet, testSet, outputPath)
  }
}
