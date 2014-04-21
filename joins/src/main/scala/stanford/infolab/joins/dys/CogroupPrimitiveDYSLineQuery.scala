/*** JoinPrimitiveDYSLineQuery.scala ***/
package stanford.infolab.joins.dys;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.PairRDDFunctions

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
class CogroupPrimitiveDYSLineQuery extends BaseDYS {

  def parseAndDoSemijoins(sc: SparkContext, dysArgs: DYSArguments):
    ListBuffer[RDD[(Long, Long)]] = {
    doSemiJoins(sc,parseEdgesIntoRDDs(sc, dysArgs.inputFiles))
  }

  private def doSemiJoins(sc: SparkContext, relations: ListBuffer[RDD[(Long, Long)]]):
	  ListBuffer[RDD[(Long, Long)]] = {
    var firstRoundSemiJoinedRelations = doSemiJoinsFromLeftToRight(sc, relations, "SJ1-R");
    doSemiJoinsFromLeftToRight(sc, firstRoundSemiJoinedRelations.reverse, "SJ2-R").reverse
  }

  private def doSemiJoinsFromLeftToRight(sc: SparkContext, relations: ListBuffer[RDD[(Long, Long)]],
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
      val resultRelationLeft = binarySemiJoin(sc, relationLeft, relationRight,
        idLeft < idRight /* reverse when going from right to left */);
      resultRelationLeft.setName(relationNamePrefix + idLeft)
      println("computed the semijoin: " + resultRelationLeft.name + " totalTime: " + 
        (System.currentTimeMillis() - timeBeforeSemijoin)  + "ms.");
//      relationI.cache();
//      semiJoinedRelations(i-1).unpersist(false)
      debugRelation(resultRelationLeft);
      semiJoinedRelations.insert(i, resultRelationLeft);
    }
    semiJoinedRelations
  }

  private def binarySemiJoin(sc: SparkContext, relationL: RDD[(Long, Long)],
    relationR: RDD[(Long, Long)], reverseResult: Boolean): RDD[(Long, Long)] = {
    relationL.cogroup(relationR).flatMap(keyLvalsRvals => {
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

  private def parseEdgesIntoRDDs(sc: SparkContext, relationFiles: Array[String]):
	  ListBuffer[RDD[(Long, Long)]] = {
    var edgesRDDList = new ListBuffer[RDD[(Long, Long)]]()
    for (i <- 0 to relationFiles.length-1) {
      val edgesRDD = sc.textFile(relationFiles(i), 2).map(
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