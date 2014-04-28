/*** JoinPrimitiveDYSLineQuery.scala ***/
package stanford.infolab.joins.dys;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import stanford.infolab.joins.JoinsArguments
import stanford.infolab.joins.JoinsAlgorithm

/**
 * Answers a line query with the distributed Yannakakis algorithm using Spark's join primitive:
 * Stores the relations as RDDs containing (Long, Long) tuples. Because it uses Spark's cogroup
 * primitive which only groups on the first column, the algorithm needs to switch the values in the
 * (Long, Long) tuples of relation R_i depending on whether R_i is being joined with R_{i+1}
 * or R_{i-1}. 
 * 
 * TODO(semih): Experiment with combining.
 * TODO(semih): Experiment with caching and unpersisting.
 * TODO(semih): Experiment with using None as the value of the relation on the right in semijoins.
 */
class DYS(joinsArgs: JoinsArguments) extends JoinsAlgorithm(joinsArgs) {

  def computeLineQuery(sc: SparkContext): RDD[_] = {
    val startTime = System.currentTimeMillis();
    var originalEdgesRDDList = parseEdgesIntoRDDs(sc);
    if (joinsArgs.cacheIntermediateResults) {
      for (originalEdgesRDD <- originalEdgesRDDList) {
        originalEdgesRDD.persist(StorageLevel.MEMORY_ONLY_SER);
      }
    }
    var semiJoinedEdgesList = originalEdgesRDDList;
    if (!joinsArgs.skipSemijoining) {
      println("performing the semijoining stage..");
      semiJoinedEdgesList = semijoin(semiJoinedEdgesList);
    }
    val finalJoin = doJoins(semiJoinedEdgesList);
    if (joinsArgs.unpersistIntermediateResults) {
      for (semijoinedEdges <- semiJoinedEdgesList) {
        semijoinedEdges.unpersist(blocking = false);
      }
    }
    finalJoin
  }
  
  def semijoin(edgeLists: ListBuffer[RDD[(Long, Long)]]):
    ListBuffer[RDD[(Long, Long)]] = {
    val semijoinedRelations = doSemiJoins(edgeLists)
    if (joinsArgs.cacheIntermediateResults) {
      for (semijoinedRelation <- semijoinedRelations) {
        semijoinedRelation.persist(StorageLevel.MEMORY_ONLY_SER);
      }
    }
    semijoinedRelations;
  }

  private def doSemiJoins(relations: ListBuffer[RDD[(Long, Long)]]):
    ListBuffer[RDD[(Long, Long)]] = {
    var firstRoundSemiJoinedRelations = doSemiJoinsFromLeftToRight(relations, "SJ1-R");
    doSemiJoinsFromLeftToRight(firstRoundSemiJoinedRelations.reverse, "SJ2-R").reverse
  }

  private def doSemiJoinsFromLeftToRight(relations: ListBuffer[RDD[(Long, Long)]],
    relationNamePrefix: String): ListBuffer[RDD[(Long, Long)]] = {
    println("doSemiJoinsFromLeftToRight called. relations.size: " + relations.size)
    var semiJoinedRelations = ListBuffer[RDD[(Long, Long)]]();

    relations(0).setName(relationNamePrefix + getRelationId(relations(0).name));
    semiJoinedRelations.insert(0, relations(0));
    debugRelation(semiJoinedRelations(0))
    for (i <- 1 to relations.size - 1) {
      var relationLeft = relations(i)
      var relationRight = semiJoinedRelations(i-1)      
      val idLeft = getRelationId(relationLeft.name)
      val idRight = getRelationId(relationRight.name)
      if (idLeft < idRight) {
        val relationLeftName = relationLeft.name
        relationLeft = relationLeft.map(keyValue => (keyValue._2, keyValue._1))
        relationLeft.setName(relationLeftName)
      } else {
        val relationRightName = relationRight.name
        relationRight = relationRight.map(keyValue => (keyValue._2, keyValue._1))
        relationRight.setName(relationRightName)
      }
      val timeBeforeSemijoin = System.currentTimeMillis();
      val resultRelationLeft = binarySemiJoin(relationLeft, relationRight,
        joinsArgs.reduceParallelism, idLeft < idRight /* reverse when going from right to left */);
      resultRelationLeft.setName(relationNamePrefix + idLeft)
      println("computed the semijoin: " + resultRelationLeft.name + " totalTime: " + 
        (System.currentTimeMillis() - timeBeforeSemijoin)  + "ms.");
      if (joinsArgs.cacheIntermediateResults) {
    	  resultRelationLeft.persist(StorageLevel.MEMORY_ONLY_SER);
    	  if (joinsArgs.unpersistIntermediateResults) {
            relationRight.unpersist(blocking=false);
    	  }
      }
      semiJoinedRelations(i-1).unpersist(false)
      debugRelation(resultRelationLeft);
      semiJoinedRelations.insert(i, resultRelationLeft);
    }
    semiJoinedRelations
  }

  private def binarySemiJoin(relationL: RDD[(Long, Long)],
    relationR: RDD[(Long, Long)], reduceParallelism: Int, reverseResult: Boolean): RDD[(Long, Long)] = {
    relationL.cogroup(relationR, reduceParallelism).flatMap(keyLvalsRvals => {
      val key = keyLvalsRvals._1;
      if (!keyLvalsRvals._2._2.isEmpty) {
        keyLvalsRvals._2._1.map(lvalue =>  {
          if (reverseResult) {
            (lvalue, key)
          } else {
             (key, lvalue)
          }
        })
      } else{
        Seq()
      }
    })
  }

  private def doJoins(relations: ListBuffer[RDD[(Long, Long)]]): RDD[(Long, (Any, Long))] = {
    println("Starting to do the joins. relations.size: " + relations.size);
	debugRelation(relations(0))
    debugRelation(relations(1))
    var relationLeft = relations(0).map(keyValue => (keyValue._2, keyValue._1))

    val timeBeforeJoin = System.currentTimeMillis();
    var intermediateJoin: RDD[(Long, (Any, Long))] = relationLeft.join(
      relations(1), joinsArgs.reduceParallelism).map(keyValue =>
        (keyValue._2._2, (keyValue._2._1, keyValue._1)));
    intermediateJoin.setName("INTERMEDIATE_JOIN_1")
    if (joinsArgs.cacheIntermediateResults) {
      intermediateJoin.cache();
    }
    println("computed intermediate join: " + intermediateJoin.name + " totalTime: " + 
        (System.currentTimeMillis() - timeBeforeJoin) + "ms.");
    debugRelation(intermediateJoin)
    if (relations.size > 2) {
      println("starting joining other relations. numRelations: " + relations.size);
      for (i <- 2 to relations.size - 1) {
        var newIntermediateJoin: RDD[(Long, (Any, Long))] = intermediateJoin.join(
          relations(i), joinsArgs.reduceParallelism).map(keyValue =>
            (keyValue._2._2, (keyValue._2._1, keyValue._1)))
        newIntermediateJoin.setName("INTERMEDIATE_JOIN_" + i)
        if (joinsArgs.cacheIntermediateResults) {
          newIntermediateJoin.cache();
          if (joinsArgs.unpersistIntermediateResults) {
            intermediateJoin.unpersist(blocking=false);
          }
        }
        println("computed intermediate join: " + newIntermediateJoin.name);
        debugRelation(newIntermediateJoin)
        intermediateJoin = newIntermediateJoin;
      }
    } else {
      println("relation size is <= 2");
    }
    intermediateJoin.setName("FINAL_JOIN")
    intermediateJoin
  }

  /**
   * Returns the index of R_i or an intermediate result from its name.
   * Relations are given names of the form: {Stage}-i.
   */
  def getRelationId(relationName: String): Int = {
    ("" + relationName(relationName.length -1)).toInt
  }

  def parseEdgesIntoRDDs(sc: SparkContext): ListBuffer[RDD[(Long, Long)]] = {
    val timeBeforeParsing = System.currentTimeMillis();
    var edgesRDDList = new ListBuffer[RDD[(Long, Long)]]()
    for (i <- 0 to joinsArgs.inputFiles.length-1) {
      val edgesRDD = sc.textFile(joinsArgs.inputFiles(i), joinsArgs.mapParallelism).map(
      line => {
        val split = line.split("\\s+")
        (split(0).toLong, split(1).toLong)
      })
      edgesRDD.setName("R" + i);
      edgesRDDList += edgesRDD
    }
    edgesRDDList
  }
}