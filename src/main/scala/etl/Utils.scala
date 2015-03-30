package etl

import org.apache.hadoop.fs.{Path, FileSystem}

import scala.collection.mutable.ListBuffer

object Utils {

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