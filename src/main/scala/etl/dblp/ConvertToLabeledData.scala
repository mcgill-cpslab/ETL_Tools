package etl.dblp

import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object ConvertToLabeledData {

  def main(args: Array[String]): Unit = {
    val titles = new ListBuffer[String]
    val years = new ListBuffer[Int]
    var title: String = null
    var year: Int = 0
    val articlePattern = """<article(.*)""".r
    val titlePattern = """<title>(.*)</title>""".r
    val yearPattern = """<year>(.*)</year>""".r
    var isArticleFlag = false
    for (line <- Source.fromFile(args(0)).getLines()) {
      line match {
        case articlePattern(x) =>
          isArticleFlag = true
        case titlePattern(newTitle) =>
          if (isArticleFlag) {
            title = newTitle
          }
        case yearPattern(newYear) =>
          if (isArticleFlag) {
            year = newYear.toInt
            titles += new String(title)
            years += year
            isArticleFlag = false
          }
        case _ =>

      }
    }
    //generate rdd
    val sc = new SparkContext
    val titlesRDD = sc.parallelize(titles)
    // generate bag of words
    val words = titlesRDD.map(str =>
      if (str.charAt(str.length - 1) == '.') {
        str.substring(0, str.length - 1)
      } else {
        str
      }).map(title => title.toLowerCase.split(" ").toSeq).cache()
    val hashingTF = new HashingTF(1000)
    val tf = hashingTF.transform(words).repartition(args(2).toInt)
    val labels = sc.parallelize(years).map(year => if (year > 2007) 0 else 1).repartition(
      args(2).toInt)
    labels.zip(tf).map{case (l, f) => LabeledPoint(l, f)}.saveAsTextFile(args(1))
  }
}
