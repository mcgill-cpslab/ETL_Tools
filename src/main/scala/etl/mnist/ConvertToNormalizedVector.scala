package etl.mnist

import java.nio.file.{StandardOpenOption, Paths, Files}
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.io.Source


import etl.vector.SparseVector

object ConvertToNormalizedVector {

  val vectorDim = 784

  def loadAsArray(trainingPath: String, testPath: String, trainingSize: Int, testSize: Int):
  (Array[Array[Double]], Array[Array[Double]]) = {
    val training = Array.fill[Array[Double]](trainingSize)(null)
    val testing = Array.fill[Array[Double]](testSize)(null)
    var cnt = 0
    for (line <- Source.fromFile(trainingPath).getLines()) {
      val newArray = line.split(",").map(_.toDouble)
      training(cnt) = newArray
      cnt += 1
    }
    for (line <- Source.fromFile(testPath).getLines()) {
      val newArray = line.split(",").map(_.toDouble)
      testing(cnt) = newArray.tail
      cnt += 1
    }
    (training, testing)
  }

  def lookupDimWithMostVariations(vectorArray: Array[Array[Double]], keptDim: Int): Array[Int] = {
    //1. work out mean
    val sum  = new Array[Double](vectorDim)
    for (dim <- 0 until vectorDim; vector <- vectorArray) {
      sum(dim) += vector(dim)
    }
    val mean = sum.map(sumOnDim => sumOnDim / vectorArray.length)
    //2. squared difference
    val squaredDifference = new Array[Double](vectorDim)
    for (dim <- 0 until vectorDim; vector <- vectorArray) {
      squaredDifference(dim) += math.pow(vector(dim) - mean(dim), 2)
    }
    //3. get variation
    val variation = squaredDifference.map(diff => diff / vectorArray.length)
    variation.zipWithIndex.sortWith((a, b) => a._1 > b._1).map(_._2).take(keptDim)
  }

  def filterVectorWithSpecifiedDim(
      trainingArray: Array[Array[Double]],
      testArray: Array[Array[Double]],
      interestedDim: mutable.HashSet[Int]): (Array[Array[Double]], Array[Array[Double]])  = {
    (trainingArray.map(vector => vector.zipWithIndex.filter(a => interestedDim.contains(a._2)).map(_._1)),
      testArray.map(vector => vector.zipWithIndex.filter(a => interestedDim.contains(a._2)).map(_._1)))
  }

  def outputFilter(
      interestedDims: Array[Int], 
      trainingArray: Array[Array[Double]],
      testArray: Array[Array[Double]],
      outputPath: String): Unit = {
    val size = vectorDim
    var cnt = 0

    trainingArray.foreach(vectorArray => {
      val filteresVectorDim = vectorArray.zipWithIndex.filter(_._1 != 0).map(_._2)
      val vector = new SparseVector(cnt, size, filteresVectorDim, vectorArray.filter(_ != 0))
      cnt += 1
      Files.write(Paths.get(outputPath + "_training"), (vector.toString + "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND)
    })

    testArray.foreach(vectorArray => {
      val filteresVectorDim = vectorArray.zipWithIndex.filter(_._1 != 0).map(_._2)
      val vector = new SparseVector(cnt, size, filteresVectorDim, vectorArray.filter(_ != 0))
      cnt += 1
      Files.write(Paths.get(outputPath + "_test"), (vector.toString + "\n").getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND)
    })
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("Usage: program training_path test_path output_path")
      sys.exit(1)
    }
    val trainingPath = args(0)
    val testPath = args(1)
    val (trainingVectors, testVectors) = loadAsArray(trainingPath, testPath, 60000, 10000)
    val mostInterestingDims = lookupDimWithMostVariations(trainingVectors, 50)
    val mostInterestingDimsSet = new mutable.HashSet[Int]()
    mostInterestingDims.foreach(dim => mostInterestingDimsSet += dim)
    val (filteredTraining, filtertedTest) = filterVectorWithSpecifiedDim(
      trainingVectors, testVectors, mostInterestingDimsSet)
    outputFilter(mostInterestingDims, filteredTraining, filtertedTest, args(1))
  }
}
