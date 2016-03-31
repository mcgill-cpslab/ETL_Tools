package etl.dblp

import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object ConvertToLabeledData {

  case class Article(year: Int, title: String)

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[Article]
    var title: String = null
    var year: Int = 0
    for (line <- Source.fromFile(args(0)).getLines()) {
      val trimmedLine = line.trim
      if (line.startsWith("<title>")) {
        title = trimmedLine.substring(7, trimmedLine.length - 8)
      } else {
        if (line.startsWith("<year>")) {
          year = trimmedLine.substring(6, trimmedLine.length - 7).toInt
          list += Article(year, title)
        }
      }
    }
    //generate rdd
    val sc = new SparkContext
    val articleRDD = sc.parallelize(list).repartition(20).cache()
    // generate tdidf
    val words = articleRDD.map(article => article.title.filter(c => (c >= 'a' && c <= 'z') ||
      (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')).split(" ").toSeq)
    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(words)
    val labeledData = articleRDD.map(article => if (article.year > 2007) 1 else 0).zip(tf).map{
      case (label, feature) => LabeledPoint(label, feature)
    }
    labeledData.saveAsTextFile(args(1))
  }
}
