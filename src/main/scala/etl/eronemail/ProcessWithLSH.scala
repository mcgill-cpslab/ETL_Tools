package etl.eronemail

import java.io.File
import java.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Random

import breeze.stats.distributions.Gaussian
import com.typesafe.config.{Config, ConfigFactory}
import etl.vector.{SparseVector, Vectors}
import org.apache.spark.SparkContext

private object SimilarityCalculator {

  def calculateSimilarity(vector1: SparseVector, vector2: SparseVector): Double = {
    require(vector1.size == vector2.size, s"vector1 size: ${vector1.size}, " +
      s"vector2 size: ${vector2.size}")
    var similarity = 0.0
    val vector1Map = new mutable.HashMap[Int, Double]
    val vector2Map = new mutable.HashMap[Int, Double]
    for (i <- 0 until vector1.indices.size) {
      vector1Map += vector1.indices(i) -> vector1.values(i)
    }
    for (i <- 0 until vector2.indices.size) {
      vector2Map += vector2.indices(i) -> vector2.values(i)
    }
    for ((idx, value) <- vector1Map) {
      similarity += {
        if (vector2Map.contains(idx)) {
          value * vector2Map(idx)
        } else {
          0.0
        }
      }
    }
    similarity
  }

  def fastCalculateSimilarity(vector1: SparseVector, vector2: SparseVector): Double = {
    require(vector1.size == vector2.size, s"vector1 size: ${vector1.size}, " +
      s"vector2 size: ${vector2.size}")
    var similarity = 0.0
    val validBits = vector1.bitVector.clone().asInstanceOf[util.BitSet]
    validBits.and(vector2.bitVector)
    var nextSetBit = validBits.nextSetBit(0)
    while (nextSetBit != -1) {
      similarity += vector1.indexToMap(nextSetBit) * vector2.indexToMap(nextSetBit)
      nextSetBit = validBits.nextSetBit(nextSetBit + 1)
    }
    similarity
  }
}


/**
 * the set of the parameters defining a hash function
 */
trait LSHFunctionParameterSet extends Serializable

/**
 * the trait defining all hash functions used in a LSH instance
 * By passing different parameter type T, we implement different LSH schema
 * @tparam T the definition of the parameter set specifying a hash function
 */
private trait LSHHashFamily[+T <: LSHFunctionParameterSet] {

  /**
   * get a set of parameters of the lsh function; essentially the user calls this method to get a
   * hash function from the family
   * @return the list of LSHTableHashChain
   */
  def pick(tableNum: Int): List[LSHTableHashChain[T]]

  /**
   * generate a hash table chain from the file
   * @param filePath the path of the file storing the hash chain
   * @param tableNum the number of hash tables*
   * @return the list of LSHTableHashChain
   */
  def generateTableChainFromFile(filePath: String, tableNum: Int): List[LSHTableHashChain[T]]
}


/**
 * the class implementing the functions chaining in one of the hash tables
 * @param chainLength the number of hash functions
 * @param chainedHashFunctions the parameter setup for one of the functions
 * @tparam T type of parameter set
 */
private abstract class LSHTableHashChain[+T <: LSHFunctionParameterSet](
     private val chainLength: Int,
     private val chainedHashFunctions: List[T]) extends Serializable {

  /**
   * calculate the index of the vector in the hash table corresponding to the set of functions
   * defined in this class
   * @param vector the vector to be indexed
   * @return the index of the vector
   */
  def compute(vector: SparseVector): Int
}

/**
 * a hash family containing functions H(v) = FLOOR((a * v  + b) / W)
 * vector a is generated according to the p-stable distribution (Gaussian Distribution)
 * @param familySize total number of functions in this family
 * @param vectorDim the vector dimensionality supported by this family
 * @param pStableMu the mu value of the Gaussian distribution
 * @param pStableSigma the sigma value of Gaussian distribution
 * @param w W selected
 * @param chainLength the length of the hash function chain
 */
private class PStableHashFamily(
                                      familySize: Int,
                                      vectorDim: Int,
                                      pStableMu: Double,
                                      pStableSigma: Double,
                                      w: Int,
                                      chainLength: Int) extends LSHHashFamily[PStableParameterSet] {

  /**
   * initialize the hash family
   * @return the Array containing all hash functions in this family
   */
  private def initHashFamily: Array[PStableParameterSet] = {
    val uniformRandomizer = new Random(System.currentTimeMillis())
    val hashFamily = new Array[PStableParameterSet](familySize)
    val gaussianDist = new Gaussian(pStableMu, pStableSigma)
    // initialize hashFamily
    for (i <- 0 until familySize) {
      // step 1: generate vector a
      val vectorADimValues = (0 until vectorDim).map(idx => (idx, gaussianDist.sample()))
      val nonZeroIdx = vectorADimValues.filter(_._2 != 0).map(_._1).toArray
      val nonZeroValues = vectorADimValues.filter(_._2 != 0).map(_._2).toArray
      val vectorA = new SparseVector(Vectors.nextVectorID, vectorDim, nonZeroIdx, nonZeroValues)
      // step 2: select b
      val b = uniformRandomizer.nextInt(w)
      // step 3: generate each hash function chain
      hashFamily(i) = new PStableParameterSet(vectorA, w, b)
    }
    hashFamily
  }

  /**
   * get a set of parameters of the lsh function; essentially the user calls this method to get a
   * hash function from the family
   * @return the list of LSHTableHashChain
   */
  override def pick(tableNum: Int): List[LSHTableHashChain[PStableParameterSet]] = {
    val uniformRandomizer = new Random(System.currentTimeMillis())
    val hashFamily = initHashFamily
    val generatedHashChains = new Array[LSHTableHashChain[PStableParameterSet]](
      tableNum)

    // generate the hash chain
    for (i <- 0 until tableNum) {
      val hashFunctionChain = (0 until chainLength).map(_ =>
        hashFamily(uniformRandomizer.nextInt(familySize))).toList
      generatedHashChains(i) = new PStableHashChain(chainLength, hashFunctionChain)
    }
    generatedHashChains.toList
  }

  /**
   * generate a hash table chain from the file
   * assumed file format (for each line)
   * vector A; b; w
   * @param filePath the path of the file storing the hash chain
   * @param tableNum the number of hash tables
   * @return the list of LSHTableHashChain
   */
  override def generateTableChainFromFile(filePath: String, tableNum: Int):
  List[LSHTableHashChain[PStableParameterSet]] = {
    val paraSetList = new ListBuffer[PStableParameterSet]
    try {
      for (line <- Source.fromFile(filePath).getLines()) {
        val Array(vectorString, bInStr, wInStr) = line.split(";")
        val vectorA = Vectors.fromString(vectorString)
        val b = bInStr.toDouble
        val w = wInStr.toInt
        paraSetList += new PStableParameterSet(
          Vectors.sparse(vectorA._1, vectorA._2, vectorA._3, vectorA._4).asInstanceOf[SparseVector],
          b, w)
      }
      val groupedParaSets = paraSetList.grouped(chainLength)
      groupedParaSets.map(paraSet => new PStableHashChain(chainLength, paraSet.toList)).toList
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }
}

/**
 * implementation of a hash chain containing function H(v) = FLOOR((a * v  + b) / W)
 * @param chainSize the length of the chain
 * @param chainedFunctions the list of the funcitons used to calculate the index of the vector
 */
private class PStableHashChain(chainSize: Int, chainedFunctions: List[PStableParameterSet])
  extends LSHTableHashChain[PStableParameterSet](chainSize, chainedFunctions) {

  require(chainSize == chainedFunctions.size, s"$chainSize, ${chainedFunctions.size}")

  /**
   * calculate the index of the vector in the hash table corresponding to the set of functions
   * defined in this class
   * each function generates an integer which is then converted into a byte array and all integers
   * are concatenated as the index of the element in the table
   * @param vector the vector to be indexed
   * @return the index of the vector
   */
  override def compute(vector: SparseVector): Int = {
    // generate integer typed index
    val ps2 = chainedFunctions.head
    val sum = SimilarityCalculator.fastCalculateSimilarity(ps2.a, vector)
    ((sum + ps2.b) / ps2.w).toInt
  }
}

/**
 * This parameter set forms the hash function
 *
 * H(v) = FLOOR((a * v  + b) / W)
 * @param a a is a d-dimensional random vector with entries chosen independently from a p-stable
 *          distribution
 * @param b b is a real number chose uniformly from [0, W]
 * @param w W is an integer which should be large enough
 *
 */
private class PStableParameterSet(val a: SparseVector, val b: Double, val w: Int)
  extends LSHFunctionParameterSet {

  override def toString: String = s"$a;$b;$w"
}


private class AngleHashFamily(
    familySize: Int,
    vectorDim: Int,
    chainLength: Int) extends LSHHashFamily[AngleParameterSet] {

  private def getNewUnitVector: SparseVector = {
    val values = {
      val arr = (for (vectorDim <- 0 until vectorDim) yield Random.nextDouble()).toArray
      arr.map(value => if (Random.nextInt(2) > 0) value else -1 * value)
    }
    val indices = values.zipWithIndex.filter{case (value, index) => value != 0}.map(_._2)
    //normailization
    val sqrSum = math.sqrt(
      values.foldLeft(0.0){case (currentSum, newNum) => currentSum + newNum * newNum})
    new SparseVector(Vectors.nextVectorID, indices.length, indices, values.map( _ / sqrSum))
  }

  private def initHashFamily: Array[AngleParameterSet] = {
    val parameters = new ListBuffer[AngleParameterSet]
    for (i <- 0 until familySize) {
      parameters += AngleParameterSet(getNewUnitVector)
    }
    parameters.toArray
  }

  /**
   * get a set of parameters of the lsh function; essentially the user calls this method to get a
   * hash function from the family
   * @return the list of LSHTableHashChain
   */
  override def pick(tableNum: Int): List[LSHTableHashChain[AngleParameterSet]] = {
    val hashFamily = initHashFamily
    val uniformRandomizer = new Random(System.currentTimeMillis())
    val generatedHashChains = new Array[LSHTableHashChain[AngleParameterSet]](tableNum)
    for (tableId <- 0 until tableNum) {
      val hashFunctionChain = (0 until chainLength).map(_ =>
        hashFamily(uniformRandomizer.nextInt(familySize))).toList
      generatedHashChains(tableId) = new AngleHashChain(chainLength, hashFunctionChain)
    }
    generatedHashChains.toList
  }

  /**
   * generate a hash table chain from the file
   * @param filePath the path of the file storing the hash chain
   * @param tableNum the number of hash tables*
   * @return the list of LSHTableHashChain
   */
  override def generateTableChainFromFile(filePath: String, tableNum: Int):
  List[LSHTableHashChain[AngleParameterSet]] = {
    val paraSetList = new ListBuffer[AngleParameterSet]
    try {
      for (vectorString <- Source.fromFile(filePath).getLines()) {
        val unitVector = Vectors.fromString(vectorString)
        paraSetList += new AngleParameterSet(
          Vectors.sparse(unitVector._1, unitVector._2, unitVector._3, unitVector._4).
            asInstanceOf[SparseVector])
      }
      val groupedParaSets = paraSetList.grouped(chainLength)
      groupedParaSets.map(paraSet => new AngleHashChain(chainLength, paraSet.toList)).toList
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }
}

private class AngleHashChain(chainSize: Int, chainedFunctions: List[AngleParameterSet])
  extends LSHTableHashChain[AngleParameterSet](chainSize, chainedFunctions) {

  private def sign(input: Double): Int = if (input <= 0) 0 else 1

  /**
   * calculate the index of the vector in the hash table corresponding to the set of functions
   * defined in this class
   * @param vector the vector to be indexed
   * @return the index of the vector
   */
  override def compute(vector: SparseVector): Int = {
    var result = 0
    for (hashFunctionId <- 0 until chainSize) {
      val signResult = sign(
        SimilarityCalculator.fastCalculateSimilarity(chainedFunctions(hashFunctionId).a,
          vector))
      result = result << 1 | signResult
    }
    result
  }
}

private case class AngleParameterSet(a: SparseVector) extends LSHFunctionParameterSet {

  override def toString: String = a.toString
}


private class LSH(conf: Config) extends Serializable {
  private val lshFamilyName: String = conf.getString("cpslab.lsh.name")
  //TODO: to implement two-level partition mechanism in PLSH, we have to expose this variable to
  // external side; we can actually fix it with Dependency Injection, etc.?
  private val tableIndexGenerators: List[LSHTableHashChain[_]] = initHashChains()

  private def initHashChains[T <: LSHFunctionParameterSet](): List[LSHTableHashChain[_]] = {
    val familySize = conf.getInt("cpslab.lsh.familySize")
    val vectorDim = conf.getInt("cpslab.lsh.vectorDim")
    val chainLength = conf.getInt("cpslab.lsh.chainLength")
    val w = conf.getInt("cpslab.lsh.w")
    val mu = conf.getInt("cpslab.lsh.mu")
    val sigma = conf.getInt("cpslab.lsh.sigma")
    val initializedChains = lshFamilyName match {
      case "angle" =>
        val family = Some(new AngleHashFamily(familySize = familySize, vectorDim = vectorDim,
          chainLength = chainLength))
        pickUpHashChains(family)
      case "pStable" =>
        val family = Some(new PStableHashFamily(familySize = familySize, vectorDim = vectorDim,
          pStableMu = mu, pStableSigma = sigma, w = w, chainLength = chainLength))
        pickUpHashChains(family)
      case x => None
    }
    if (initializedChains.isDefined) {
      initializedChains.get
    } else {
      List()
    }
  }

  private def pickUpHashChains[T <: LSHFunctionParameterSet](lshFamily: Option[LSHHashFamily[T]]):
  Option[List[LSHTableHashChain[T]]] = {
    require(lshFamily.isDefined, s"$lshFamilyName is not a valid family name")
    val tableNum = conf.getInt("cpslab.lsh.tableNum")
    val generateMethodOfHashFamily = conf.getString("cpslab.lsh.generateMethod")
    lshFamily.map(lshHashFamily => {
      if (generateMethodOfHashFamily == "default") {
        lshHashFamily.pick(tableNum)
      } else if (generateMethodOfHashFamily == "fromfile"){
        lshHashFamily.generateTableChainFromFile(conf.getString("cpslab.lsh.familyFilePath"),
          tableNum)
      } else {
        null
      }
    })
  }


  /**
   * calculate the index of the vector in tables, the index in each table is represented as a
   * byte array
   * @param vector the vector to be indexed
   * @return the index of the vector in tables, the order corresponds to the validTableIDs parameter
   */
  def calculateIndex(vector: SparseVector): Array[Int] = {
    (for (i <- 0 until tableIndexGenerators.size)
      yield tableIndexGenerators(i).compute(vector)).toArray
  }
}

object ProcessWithLSH {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("Usage: program conf_path")
      sys.exit(1)
    }
    //configuration
    val conf = ConfigFactory.parseFile(new File(args(0)))
    val filePath = conf.getString("inputFilePath")

    //build lsh
    val lsh = new LSH(conf)

    val sc = new SparkContext()
    val rawVectorStrRdd = sc.textFile(filePath)
    val vectorTupleWithoutID = rawVectorStrRdd.map(Vectors.fromStringWithoutVectorID)
    val vectorTupleWithID = vectorTupleWithoutID.zipWithUniqueId()
    val vectorRDD = vectorTupleWithID.map{case (vectorTuple, id) =>
      new SparseVector(id.toInt, vectorTuple._1, vectorTuple._2, vectorTuple._3)}.cache()
    //calculate with LSH
    val vectorWithLSHResult = vectorRDD.map(vector  =>
      (vector.toString, {
        lsh.calculateIndex(vector).mkString(",")
      }))
    vectorWithLSHResult.repartition(96).saveAsTextFile("emailVectorWithLSH")
    //output statistical info
    val statistical = vectorRDD.
      map(vector => (vector.vectorId, lsh.calculateIndex(vector))).flatMap{
      case (vectorId, lshBucketIds) =>
        lshBucketIds.indices.map(index => (index, (lshBucketIds(index), vectorId)))
    }.groupBy(_._1).map{case (tableId, lshBuckets) =>
      (tableId,
        lshBuckets.map(_._2).groupBy(_._1).map(
          tableDistribution => (tableDistribution._1, tableDistribution._2.size)).toList.sortBy(_._1))}
    statistical.repartition(96).saveAsTextFile("statistical")
  }
}
