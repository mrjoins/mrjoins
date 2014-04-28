package stanford.infolab.joins

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.log4j.lf5.LogLevel

abstract class JoinsAlgorithm(joinsArgs: JoinsArguments) extends Logging with Serializable {

  /**
   * Computes a line query.
   */
  def computeLineQuery(sc: SparkContext): RDD[_];

  def debugRelation[T](relation: RDD[T]) {
    if (joinsArgs.logLevel == LogLevel.DEBUG) {
      log.info("\nPRINTING RELATION " + relation.name + " size: " + relation.count() + "\n" +
        relation.collect.deep.mkString("\n") + "\nPRINTED RELATION " + relation.name)
    }
  }
}