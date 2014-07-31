package stanford.infolab.joins.shares

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import scala.collection.immutable.HashMap
import stanford.infolab.joins.JoinsArguments
import stanford.infolab.joins.JoinsAlgorithm

/**
* Executes an n-way join using Shares algorithm
*/
abstract class BaseShares(joinsArgs: JoinsArguments)
  extends JoinsAlgorithm(joinsArgs) {

  def computeQuery(sc: SparkContext): RDD[_] = {
    val NUM_RELATIONS: Int = joinsArgs.m
    val NUM_REDUCERS: Int = joinsArgs.reduceParallelism
    val NUM_DIMENSIONS: Int = joinsArgs.m - 1
    val DIMENSION_SIZE = Math.pow(NUM_REDUCERS.toDouble, 1.0 / NUM_DIMENSIONS.toDouble).toInt

    val relations = parseEdgesIntoRDDs(sc);
    val dataSet = sc.union(relations)

    val keySpace: Array[(Short, Array[Byte])] = computeReduceKeySpace(NUM_DIMENSIONS, DIMENSION_SIZE,
      NUM_REDUCERS)
    println("keySpace size: " + keySpace.size);

    def getReduceKeysForTuple(tuple: (Byte, Long, Long)): List[(Short, (Byte, Long, Long))] = {

      def hashValue(x: Long) = x % DIMENSION_SIZE

      def validKey(reduceKey: (Short, Array[Byte]), tuple: (Byte, Long, Long)): Boolean = tuple._1 match {
        case 0 => hashValue(tuple._3) == reduceKey._2(tuple._1)
        // NUM_DIMENSIONS = NUM_RELATIONS - 1, which is the index of the last relation in the chain of joins
        case NUM_DIMENSIONS => hashValue(tuple._2) == reduceKey._2.last
        case _ => hashValue(tuple._2) == reduceKey._2(tuple._1 - 1) && hashValue(tuple._3) == reduceKey._2(tuple._1)
      }

      (for (key <- keySpace; if validKey(key, tuple)) yield {
        (key._1, tuple)
      }).toList
    }
    
    val flatMappedSet = dataSet.flatMap(tuple => getReduceKeysForTuple(tuple))
    val groupedByKeySet = flatMappedSet.groupByKey(joinsArgs.reduceParallelism)
    groupedByKeySet.flatMapValues(performLocalJoin);
  }

  // Computes the entire reduce key space
  def computeReduceKeySpace(numDimensions: Int, dimensionSize: Int, numReducers: Int): Array[(Short, Array[Byte])] = {
    def extractDimensionRepresentation(value: Int): Array[Byte] = {
      var remaining = value
      var arr: Array[Byte] = Array()
      for (j <- (numDimensions - 1) to 0 by -1) {
        val base = Math.pow(dimensionSize, j)
        var temp = remaining / base.toInt
        arr = arr :+ temp.toByte
        remaining = remaining % base.toInt

      }
      arr
    }
    (for (i <- 0 until numReducers) yield {
      (i.toShort, extractDimensionRepresentation(i))
    }).toArray
  }

  def performLocalJoin(arrBuf: Seq[(Byte, Long, Long)]): Seq[Array[Long]];

  def parseEdgesIntoRDDs(sc: SparkContext): List[RDD[(Byte, Long, Long)]] = {
    (for (i <- 0 until joinsArgs.m) yield {
      sc.textFile(joinsArgs.inputFiles(i), 120).map(line => { //TODO: Parameterize
          val Array(x, y) = line.split("\\s+").map(_.toLong)
          (i.toByte, x, y)
      })
    }).toList
  }
}
