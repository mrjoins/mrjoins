package stanford.infolab.joins.utils

import java.io.PrintWriter
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import scala.util.Random

/**
 * Generates a random relation with k tuples and n attributes and domain sizes for each attribute.
 * Each tuple is generated randomly, and currently each attribute is an integer.
 */
object RelationGenerator {

  def main2(args: Array[String]) {
    val rgArgs = new RelationGeneratorArguments(args);

    val writer = new BufferedWriter(new FileWriter(new File(rgArgs.outputFile)))
    val random = new Random()
    for (i <- 1 to rgArgs.numTuples) {
      for (j <- 0 to rgArgs.domainSizes.size - 1) {
        if (j > 0) {
          writer.write("\t");
        }	
        writer.write(nextLong(random, rgArgs.domainSizes(j)).toString)
//          Random.nextInt(rgArgs.domainSizes(j)).toString);
      }
      writer.write("\n")
    }
    writer.close();
  }

  def nextLong(rng: Random, n: Long): Long = {
    // error checking and 2^x checking removed for simplicity.
    var bits, longVal: Long = -1;
    do {
      bits = (rng.nextLong() << 1) >>> 1;
      longVal = bits % n;
    } while (bits - longVal + (n - 1) < 0L);
    return longVal + 4294967296L;
  }
}