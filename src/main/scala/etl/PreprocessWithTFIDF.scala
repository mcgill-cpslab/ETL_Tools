package etl

import java.io.{InputStreamReader, BufferedReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.feature.IDF
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

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

  /**
   * map each file specified in the allFilesPath to a single line (a string)
   * @param allFilesPath list of file path
   * @return RDD of the file content (each string per file)
   */
  def mapEachFileToSingleLine(sc: SparkContext,
                              allFilesPath: ListBuffer[String],
                              outputPartitionNum: Int): RDD[String] = {
    var allFileContentRDD: RDD[String] = null
    val allFilesPathRDD = sc.parallelize(allFilesPath, outputPartitionNum)
    allFileContentRDD = allFilesPathRDD.map(sourcePathString =>  {
      val hadoopConf = new Configuration()
      val sourcePath = new Path(sourcePathString)
      val sourceFs = sourcePath.getFileSystem(hadoopConf)
      val fileHandler = sourceFs.open(sourcePath)
      val isr = new InputStreamReader(fileHandler)
      val br = new BufferedReader(isr)
      var retStr = ""
      var line = ""
      while (line != null) {
        line = br.readLine()
        if (filterNonSenseLines(line)) {
          retStr += (line + " ")
        }
      }
      fileHandler.close()
      isr.close()
      br.close()
      retStr
    })
    allFileContentRDD
  }

  def computeTFIDFVector(sc: SparkContext, documents: RDD[String]): RDD[Vector] = {
    val docs = documents.map(_.split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(docs)
    tf.cache()
    val idf = new IDF().fit(tf)
    idf.transform(tf)
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
    val fileContentRDD = mapEachFileToSingleLine(sc, allFilesToProcess, args(2).toInt)
    val tfidfRDD = computeTFIDFVector(sc, fileContentRDD)
    tfidfRDD.saveAsTextFile(args(1))
  }
}