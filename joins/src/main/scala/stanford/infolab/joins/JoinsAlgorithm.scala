package stanford.infolab.joins

import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.rdd.EmptyRDD
import org.apache.spark.rdd.EmptyRDD
import org.apache.commons.lang.NotImplementedException
import org.apache.log4j.Level

abstract class JoinsAlgorithm(joinsArgs: JoinsArguments) extends Logging with Serializable {

  /**
   * Computes a line query.
   */
  def computeQuery(sc: SparkContext): RDD[_];
  
  def debugRelation[T](relation: RDD[T]) {
    
    if (joinsArgs.logLevel == Level.DEBUG) {
      val collectedRelation = relation.collect
      log.info("\nPRINTING RELATION " + relation.name + " size: " + relation.count() + "\n" +
        collectedRelation.deep.mkString("\n") + "\nPRINTED RELATION " + relation.name)
    }
  }
}