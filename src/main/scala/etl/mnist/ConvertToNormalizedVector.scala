package etl.mnist

import java.nio.file.{StandardOpenOption, Paths, Files}
import java.nio.charset.StandardCharsets

import scala.collection.mutable
import scala.io.Source


import etl.vector.SparseVector

object ConvertToNormalizedVector {

  val vectorDim = 784

  def loadAsArray(inputPath: String, lineCount: Int): Array[Array[Double]] = {
    val ret = Array.fill[Array[Double]](lineCount)(null)
    var cnt = 0
    for (line <- Source.fromFile(inputPath).getLines()) {
      val newArray = line.split(",").map(_.toDouble)
      ret(cnt) = newArray
      cnt += 1
    }
    ret
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
    variation.zipWithIndex.sortWith((a, b) => a._1 > b._1).map(_._2)
  }

  def filterVectorWithSpecifiedDim(
      vectorArray: Array[Array[Double]],
      interestedDim: mutable.HashSet[Int]): Array[Array[Double]] = {
    vectorArray.map(vector => vector.zipWithIndex.filter(a => interestedDim.contains(a._2)).map(_._1))
  }

  def outputFilter(interestedDims: Array[Int], vectorArrays: Array[Array[Double]], outputPath: String): Unit = {
    val size = vectorDim
    var cnt = 0

    vectorArrays.foreach(vectorArray => {
      val vector = new SparseVector(cnt, size, interestedDims, vectorArray)
      cnt += 1
      Files.write(Paths.get(outputPath), vector.toString.getBytes(StandardCharsets.UTF_8),
        StandardOpenOption.APPEND)
    })
  }

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: program input_path  output_path")
      sys.exit(1)
    }
    val inputPath = args(0)
    val vectorArray = loadAsArray(inputPath, 60000)
    val mostInterestingDims = lookupDimWithMostVariations(vectorArray, 50)
    val mostInterestingDimsSet = new mutable.HashSet[Int]()
    mostInterestingDims.foreach(dim => mostInterestingDimsSet += dim)
    val vectorWithOnlyInterestedDims = filterVectorWithSpecifiedDim(vectorArray, mostInterestingDimsSet)
    outputFilter(mostInterestingDims, vectorWithOnlyInterestedDims, args(1))
  }
}
