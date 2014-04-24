package org.infolab.joins

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
/**
* Executes a three-way join using Shares algorithm
*/
object Shares {

  val ROOT_K = 3

  def main(args: Array[String]) {
    
    val sharesArgs = new SharesArguments(args);
    val NUM_RELATIONS = sharesArgs.m - 1

    val sc = new SparkContext(sharesArgs.sparkMasterAddr, "Shares",
      System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass))
    val relations = (for (i <- 0 to NUM_RELATIONS) yield {
      sc.textFile(sharesArgs.inputFiles(i)).map(line => i +: line.split(" ").dropRight(1).map(_.toInt))
    }).toList

    val dataSet = sc.union(relations)

    def extractKey(value: Array[Int]): List[(String, Array[Int])] = {
      def hashValue(x: Int) = x % ROOT_K

      def matchValue(value: Array[Int]): List[(String, Array[Int])] = value(0) match {
        // R(a,b) => (h(b), x for any x)
        case 0 => (for (i <- 0 until ROOT_K) yield {
            (hashValue(value(2)) + "-" + i, value)
          }).toList
        // T(c, d) => (y, h(c) for any y)
        case NUM_RELATIONS => (for (i <- 0 until ROOT_K) yield {
            (i + "-" + hashValue(value(1)), value)
          }).toList
        // S(b,c) => (h(b), h(c))
        case _ => List((hashValue(value(1)) + "-" + hashValue(value(2)), value))
      }
      matchValue(value) 
    }

    dataSet.flatMap(arr => extractKey(arr))
    .groupByKey
    .mapValues(arrBuf => arrBuf.groupBy(
        arr => arr(0)
      )
    )
    .collect().foreach {
      case (a) => {
        val map = a._2
        if (map.keySet.size == ROOT_K) {
          val tuples = map(0).map(x => x.drop(1)) // tuples and otherTuples are ArrayBuffers
          val otherTuples = map(1).map(x => x.drop(1))

          var result = 
            for (tuple <- tuples; otherTuple <- otherTuples; if (tuple.last == otherTuple(0))) yield {
              tuple ++ otherTuple.tail 
            }


          for (i <- 2 until ROOT_K) {
            val moreTuples = map(i).map(x => x.drop(1))
            result = 
              for (tuple <- result; otherTuple <- moreTuples; if (tuple.last == otherTuple(0))) yield {
                tuple ++ otherTuple.tail 
              }
          }
          for (tuple <- result) {
            print(tuple.deep.mkString(","))
            println("\n")
          }
        }
      }
    }
  }
}
