package etl.dblp

import scala.collection.mutable.ListBuffer
import scala.io.Source

import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

case class Article(year: Int, title: String)

object ConvertToLabeledData {

  def main(args: Array[String]): Unit = {
    val list = new ListBuffer[Article]
    var title: String = null
    var year: Int = 0
    val titlePattern = """<title>(.*)</title>""".r
    val yearPattern = """<year>(.*)</year>""".r
    for (line <- Source.fromFile(args(0)).getLines()) {
      line match {
        case titlePattern(newTitle) =>
          title = newTitle
        case yearPattern(newYear) =>
          year = newYear.toInt
          list += Article(year, new String(title))
        case _ =>

      }
    }
    println(list.size + "===========")
    //generate rdd
    val sc = new SparkContext
    val articleRDD = sc.parallelize(list).repartition(args(2).toInt).cache()
    println(articleRDD.count())
    // generate bag of words
    val words = articleRDD.map(article => article.title.split(" ").toSeq.map(_.toLowerCase))
    val hashingTF = new HashingTF(1000)
    val tf = hashingTF.transform(words)
    println(tf.first().toSparse.size + "=====")
    val labeledData = articleRDD.map(article => if (article.year > 2007) 1 else 0).zip(tf).map{
      case (label, feature) => LabeledPoint(label, feature)
    }
    labeledData.saveAsTextFile(args(1))
  }
}
