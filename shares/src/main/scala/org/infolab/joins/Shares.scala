package org.infolab.joins

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
/**
* Executes an n-way join using Shares algorithm
*/
object Shares {

  def main(args: Array[String]) {
    
    val sharesArgs = new SharesArguments(args);

    val NUM_RELATIONS: Int = sharesArgs.m
    val NUM_REDUCERS: Int = sharesArgs.numReducers
    val NUM_DIMENSIONS: Int = sharesArgs.m - 1
    val DIMENSION_SIZE = Math.pow(sharesArgs.numReducers.toDouble, 1.0 / NUM_DIMENSIONS.toDouble).toInt

    val sc = new SparkContext(sharesArgs.sparkMasterAddr, "Shares",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val relations = (for (i <- 0 until NUM_RELATIONS) yield {
      sc.textFile(sharesArgs.inputFiles(i)).map(line => {
          val Array(x, y) = line.split("\\s+").map(_.toLong)
          (i.toByte, x, y)
      })
    }).toList

    val dataSet = sc.union(relations)

    val reduceKeySpace: Array[(Short, Array[Byte])] = {
        def extractDimensionRepresentation(value: Int): Array[Byte] = {
          var remaining = value
          var arr: Array[Byte] = Array()
          for (j <- (NUM_DIMENSIONS - 1) to 0 by -1) {
            val base = Math.pow(DIMENSION_SIZE, j)
            var temp = remaining / base.toInt
            arr = arr :+ temp.toByte
            remaining = remaining % base.toInt
            
          }
          arr
        }
        (for (i <- 0 until NUM_REDUCERS) yield {
          (i.toShort, extractDimensionRepresentation(i))
        }).toArray
    }
    val keySpace: Array[(Short, Array[Byte])] = reduceKeySpace

    def findKeysInKeySpace(tuple: (Byte, Long, Long)): List[(Short, (Byte, Long, Long))] = {

      def hashValue(x: Long) = x % DIMENSION_SIZE

      def validKey(reduceKey: (Short, Array[Byte]), tuple: (Byte, Long, Long)): Boolean = tuple._1 match {
        case 0 => hashValue(tuple._3) == reduceKey._2(tuple._1)
        // NUM_DIMENSIONS = NUM_RELATIONS - 1, which is the index of the last relation in the chain of joins
        case NUM_DIMENSIONS => hashValue(tuple._2) == reduceKey._2.last
        case _ => hashValue(tuple._2) == reduceKey._2(tuple._1 - 1) && hashValue(tuple._3) == reduceKey._2(tuple._1)
      }

      (for(key <- keySpace; if validKey(key, tuple)) yield {
          (key._1, tuple)
      }).toList
    }

    val m = dataSet.flatMap(tuple => findKeysInKeySpace(tuple)).groupByKey
    .flatMapValues(arrBuf => {
      val map = arrBuf.groupBy(
        arr => arr._1
      )
      var result: Seq[Array[Long]] = Seq.empty
      if (map.keySet.size == NUM_RELATIONS) {
        val tuples = map(0).map(x => Array(x._2, x._3)) // tuples and otherTuples are ArrayBuffers
        val otherTuples = map(1).map(x => Array(x._2, x._3))

        result = 
          for (tuple <- tuples; otherTuple <- otherTuples; if (tuple.last == otherTuple.head)) yield {
            tuple ++ otherTuple.tail 
          }

        for (i <- 2 until NUM_RELATIONS) {
          val moreTuples = map(i.toByte).map(x => Array(x._2, x._3))
          result = 
            for (tuple <- result; otherTuple <- moreTuples; if (tuple.last == otherTuple.head)) yield {
              tuple ++ otherTuple.tail 
            }
        }
      }
      result
    })
    println(m.count)
  }
}
