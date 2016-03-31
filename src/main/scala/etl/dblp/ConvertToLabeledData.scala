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
    var state = 0 // 0 - for title 1 - for year
    var title: String = null
    var year: Int = 0
    for (line <- Source.fromFile(args(0)).getLines()) {
      if (line.startsWith("<title>")) {
        require(state == 0)
        title = line.substring(7, line.length - 8 + 1)
        state = 1
      } else {
        if (line.startsWith("<year>")) {
          require(state == 1)
          year = line.substring(6, line.length - 7 + 1).toInt
          list += Article(year, title)
          state = 0
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
