/*** BaseDYS.scala ***/
package stanford.infolab.joins.dys;

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import scala.collection.mutable.ListBuffer
import org.apache.spark.Logging

/**
 * Base class for different DYS implementations.
 */
abstract class BaseDYS extends Logging {
 
  def computeLineQuery(sc: SparkContext, dysArgs: DYSArguments) {
    println("Computing line query using RDD[(Int, Long, Long)] rdds and unioning RDDs.");
    val startTime = System.currentTimeMillis();
    val semiJoinedEdgesList = parseAndDoSemijoins(sc, dysArgs);
    val finalJoin = doJoins(sc, semiJoinedEdgesList);
    println("\nFINAL JOIN.  size: " + finalJoin.count() + "\n");
    println("Finished Join. Total time: " + (System.currentTimeMillis() - startTime) + "ms.");
    if (dysArgs.outputFile == "") {
    	println("Output file not specified. So not saving the results as output file.")
    } else {
      val timeBeforeWriting = System.currentTimeMillis(); 
      finalJoin.saveAsTextFile(dysArgs.outputFile);
      println("Total time to write the output: " + (System.currentTimeMillis() - timeBeforeWriting)
        + "ms.");
    }
//      finalJoin.collect.deep.mkString("\n")
//      + "\nPRINTED FINAL JOIN.")
//    debugRelation(finalJoin)
  }

  def parseAndDoSemijoins(sc: SparkContext, dysArgs: DYSArguments):
    ListBuffer[RDD[(Long, Long)]]
  
  def doJoins(sc: SparkContext, relations: ListBuffer[RDD[(Long, Long)]]):
    RDD[(Long, (Any, Long))] = {
    println("Starting to do the joins. relations.size: " + relations.size);
	debugRelation(relations(0))
    debugRelation(relations(1))
    var relationLeft = relations(0).map(keyValue => (keyValue._2, keyValue._1))
    val timeBeforeJoin = System.currentTimeMillis();
    var intermediateJoin: RDD[(Long, (Any, Long))] = relationLeft.join(relations(1)).map(keyValue => 
        (keyValue._2._2, (keyValue._2._1, keyValue._1)));
    intermediateJoin.setName("INTERMEDIATE_JOIN_1")
    println("computed intermediate join: " + intermediateJoin.name + " totalTime: " + 
        (System.currentTimeMillis() - timeBeforeJoin) + "ms.");
    debugRelation(intermediateJoin)
    if (relations.size > 2) {
      println("starting joining other relations. numRelations: " + relations.size);
      for (i <- 2 to relations.size - 1) {
        intermediateJoin = intermediateJoin.join(relations(i)).map(keyValue =>
          (keyValue._2._2, (keyValue._2._1, keyValue._1)))
        intermediateJoin.setName("INTERMEDIATE_JOIN_" + i)
        println("computed intermediate join: " + intermediateJoin.name);
        debugRelation(intermediateJoin)
      }
    } else {
      println("relation size is <= 2");
    }
    intermediateJoin.setName("FINAL_JOIN")
    intermediateJoin
  }
  
  def debugRelation[T](relation: RDD[T]) {
//    log.info("\nPRINTING RELATION " + relation.name + " size: " + relation.count() + "\n" +
//      relation.collect.deep.mkString("\n")
//      + "\nPRINTED RELATION " + relation.name)
  }
  
  /**
   * Returns the index of R_i or an intermediate result from its name.
   * Relations are given names of the form: {Stage}-i
   */
  def getRelationId(relationName: String): Int = {
    ("" + relationName(relationName.length -1)).toInt
  }
}