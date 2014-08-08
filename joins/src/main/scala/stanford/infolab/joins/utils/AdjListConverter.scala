package stanford.infolab.joins.utils

import java.io.BufferedReader
import java.io.FileReader
import java.io.File
import scala.io.Source
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import java.io.BufferedWriter
import java.io.FileWriter
import scala.collection.mutable.MutableList
import scala.collection.mutable.ArrayBuffer

object AdjListConverter {

    def main2(args: Array[String]) {
      val adjlists = new HashMap[Long, ArrayBuffer[Long]]()
      var source, destination = -1L
      var split: Array[String] = null
      var adjlist: ArrayBuffer[Long] = null
      for(line <- Source.fromFile(args(0)).getLines()) {
        split = line.split("\\s+")
        source = split(0).toLong
        destination = split(1).toLong
        if (!adjlists.contains(source)) {
          adjlists(source) = new ArrayBuffer[Long]()
        }
        adjlists(source) += destination
      }
      val writer = new BufferedWriter(new FileWriter(new File(args(1))))
      for (adjList <- adjlists) {
        writer.write(adjList._1.toString);
        writer.write("\t");
        writer.write(adjList._2.mkString(" "));
        writer.write("\n");
      }
      writer.close();
//    val random = new Random()
//    for (i <- 1 to rgArgs.numTuples) {
//      for (j <- 0 to rgArgs.domainSizes.size - 1) {
//        if (j > 0) {
//          writer.write("\t");
//        } 
//        writer.write(nextLong(random, rgArgs.domainSizes(j)).toString)
////          Random.nextInt(rgArgs.domainSizes(j)).toString);
//      }
//      writer.write("\n")
//    }
//    writer.close();
    }

}