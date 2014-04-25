package org.infolab.joins

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
/**
* Executes an n-way join using Shares algorithm
*/
object Shares {

  // TODO: parameterize this

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
          val Array(x, y) = line.split(" ").map(_.toLong)
          (i.toByte, x, y)
      })
    }).toList

    //val dataSet = sc.union(relations)
    //dataSet.collect().foreach {
    //  case (a) => {
    //      println(a._1)
    //    }
    //}
    val reduceKeySpace: Array[(Short, Array[Byte])] = {
        def extractDimensionRepresentation(value: Int): Array[Byte] = {
          var remaining = value
          var arr: Array[Byte] = Array()
          for (j <- (NUM_DIMENSIONS - 1) to 0) {
            val base = Math.pow(DIMENSION_SIZE, j)
            var temp = remaining / base.toInt
            arr = arr :+ temp.toByte
            print(arr.deep.mkString(":"))
            remaining = remaining % base.toInt
            
          }
          arr
        }

        (for (i <- 0 until NUM_REDUCERS) yield {
          (i.toShort, extractDimensionRepresentation(i))
        }).toArray
    }
    //println(reduceKeySpace.deep.mkString(","))
    reduceKeySpace.foreach {
      case (a) => {
      print(a._1)
      print("; ")
      print(a._2.deep.mkString(","))
      println("\n")
      }
    }

    //def extractKey(value: (Byte, Long, Long)): Array[(Short, (Byte, Long, Long))] = {
    //  def hashValue(x: Long) = x % DIMENSION_SIZE
    //  
    //  def shouldMap(key: Array[Short], value: (Byte, Long, Long)): Boolean = true






    //    //  (for (i <- 0 to Math.pow(NUM_RELATIONS.toInt, NUM_REDUCERS.toDouble)) yield {
    //    //    (i.toShort, Array.fill[Byte](NUM_RELATIONS)(0))
    //    //  }
    //    //).toArray
    //  }
    //  reduceKeySpace.flatMap(key => {
    //      if (shouldMap(key, value)) {
    //        Seq((key(0), value))
    //      } else {
    //        Seq()
    //      }
    //    })
    //  //def matchValue(value: (Short, Long, Long)): List[(String, (Byte, Long, Long))] = value(0) match {
      //  // R(a,b) => (h(b), x for any x)
      //  case 0 => (for (i <- 0 until DIMENSION_SIZE) yield {
      //      (hashValue(value(2)) + "-" + i, value)
      //    }).toList
      //  // T(c, d) => (y, h(c) for any y)
      //  case NUM_RELATIONS => (for (i <- 0 until DIMENSION_SIZE) yield {
      //      (i + "-" + hashValue(value(1)), value)
      //    }).toList
      //  // S(b,c) => (h(b), h(c))
      //  case _ => List((hashValue(value(1)) + "-" + hashValue(value(2)), value))
      //}
      //matchValue(value) 
    //dataSet.flatMap(arr => extractKey(arr))
//    .groupByKey
//    .mapValues(arrBuf => arrBuf.groupBy(
//        arr => arr(0)
//      )
//    )
//    .collect().foreach {
//      case (a) => {
//        val map = a._2
//        if (map.keySet.size == DIMENSION_SIZE) {
//          val tuples = map(0).map(x => x.drop(1)) // tuples and otherTuples are ArrayBuffers
//          val otherTuples = map(1).map(x => x.drop(1))
//
//          var result = 
//            for (tuple <- tuples; otherTuple <- otherTuples; if (tuple.last == otherTuple(0))) yield {
//              tuple ++ otherTuple.tail 
//            }
//
//          for (i <- 2 until DIMENSION_SIZE) {
//            val moreTuples = map(i).map(x => x.drop(1))
//            result = 
//              for (tuple <- result; otherTuple <- moreTuples; if (tuple.last == otherTuple(0))) yield {
//                tuple ++ otherTuple.tail 
//              }
//          }
//          for (tuple <- result) {
//            print(tuple.deep.mkString(","))
//            println("\n")
//          }
//        }
//      }
//    }
  }
}
