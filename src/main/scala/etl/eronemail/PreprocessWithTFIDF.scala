package etl.eronemail

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import etl.Utils
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector, Vector}
import org.apache.spark.rdd.RDD

object PreprocessWithTFIDF {

  private def filterNonSenseLines(line: String): Boolean = {
    if (line == null || line.startsWith("Message-ID") || line.startsWith("Date") ||
      line.startsWith("From") ||
      line.startsWith("To") || line.startsWith("Subject") || line.startsWith("Mime-Version") ||
      line.startsWith("Content-Type") || line.startsWith("Content-Transfer-Encoding") ||
      line.startsWith("X-From") || line.startsWith("X-To") || line.startsWith("X-cc") ||
      line.startsWith("X-bcc") || line.startsWith("X-Folder") || line.startsWith("X-Origin") ||
      line.startsWith("X-FileName") || line.length < 2) {
      return false
    }
    true
  }

  private def filterMostFrequentWords(fileContent: RDD[String], threshold: Double):
      (RDD[Seq[String]], Int) = {
    // do word count
    val wordRDD = fileContent.map(line => line.split(" ")).cache()
    val wordCountRDD = wordRDD.flatMap(wordArray => wordArray)
      .map(word => (word, 1))
      .reduceByKey(_ + _).sortBy(wordKeyFrequency => wordKeyFrequency._2, ascending = false)
    val wordTotalNumber = wordCountRDD.count()
    val mostFrequentWordsSet = {
      val hSet = new mutable.HashSet[String]
      wordCountRDD.take((wordTotalNumber * threshold).toInt).foreach(word => hSet.add(word._1))
      hSet
    }
    (wordRDD.map(wordsArray => {
      wordsArray.filter(mostFrequentWordsSet.contains).toSeq
    }).filter(_.length >= 1), mostFrequentWordsSet.size)
  }

  def computeTFIDFVector(sc: SparkContext, docs: RDD[Seq[String]], numFeatures: Int):
      RDD[SparseVector] = {
    val hashingTF = new HashingTF(numFeatures)
    val tf: RDD[Vector] = hashingTF.transform(docs)
    tf.cache()
    val idf = new IDF().fit(tf)
    idf.transform(tf).map(_.asInstanceOf[SparseVector]).map(vector => {
      val values = vector.values
      val squareSum = math.sqrt(values.foldLeft(0.0){case (sum, weight) => sum + weight * weight})
      val normalizedValues = values.map(x => x / squareSum)
      new SparseVector(vector.size, vector.indices, normalizedValues)
    })
  }

  private def filterTFIDFVectors(sc: SparkContext, tfidfSet: RDD[SparseVector]):
      RDD[SparseVector] = {
    // get the most important words
    val sortedImportance = tfidfSet.mapPartitions(sparseVectors => {
      val idfMap = new mutable.HashMap[Int, Double]
      for (vector <- sparseVectors; i <- 0 until vector.indices.length) {
        idfMap += (vector.indices(i) -> vector.values(i))
      }
      idfMap.toSeq.sortWith((x1, x2) => x1._2 > x2._2).take(
        (idfMap.size * 0.005).toInt).iterator
    }).sortBy(x => x._2, ascending = false)
    val totalCount = sortedImportance.count()
    val availableFieldsArray = sortedImportance.take((0.005 * totalCount).toInt).map(_._1)
    tfidfSet.map(vector => {
      val indices = new ListBuffer[Int]
      val values = new ListBuffer[Double]
      for (i <- 0 until vector.indices.length) {
        val idx = vector.indices(i)
        if (availableFieldsArray.contains(idx)) {
          indices += idx
          values += vector.values(i)
        }
      }
      new SparseVector(availableFieldsArray.length, indices.toArray, values.toArray)
    }).filter(v => v.indices.length >= 1)
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
    val fileContentRDD = Utils.mapEachFileToSingleLine(sc,
      allFilesToProcess, args(2).toInt, filterNonSenseLines)
    val (filteredFileContentRDD, numFeatures) = filterMostFrequentWords(fileContentRDD, 0.005)
    val tfidfRDD = computeTFIDFVector(sc, filteredFileContentRDD, numFeatures)
    tfidfRDD.zipWithIndex().saveAsTextFile(args(1))
  }
}