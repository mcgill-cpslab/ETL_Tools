package etl

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object Utils {

  /**
   * map each file specified in the allFilesPath to a single line (a string)
   * @param allFilesPath list of file path
   * @return RDD of the file content (each string per file)
   */
  def mapEachFileToSingleLine(sc: SparkContext,
                              allFilesPath: ListBuffer[String],
                              outputPartitionNum: Int,
                              filterNonSenseLines: String => Boolean): RDD[String] = {
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

  // TODO: change to functional style
  def getAllFilePath(fs: FileSystem, rootPath: Path, list: ListBuffer[String]): Unit = {
    val fileStatus = fs.getFileStatus(rootPath)
    //list += fileStatus.getPath.toString
    if (fileStatus.isDirectory) {
      val allContainedFiles = fs.listStatus(rootPath)
      for (file <- allContainedFiles) {
        getAllFilePath(fs, file.getPath, list)
      }
    } else {
      if (!fileStatus.getPath.toString.contains(".DS_Store")) {
        list += fileStatus.getPath.toString
      }
    }
  }

  // TODO: change to functional style
  def getAllDirAndFilePath(fs: FileSystem, rootPath: Path, list: ListBuffer[String]): Unit = {
    val fileStatus = fs.getFileStatus(rootPath)
    list += fileStatus.getPath.toString
    if (fileStatus.isDirectory) {
      val allContainedFiles = fs.listStatus(rootPath)
      for (file <- allContainedFiles) {
        getAllDirAndFilePath(fs, file.getPath, list)
      }
    }
  }
}