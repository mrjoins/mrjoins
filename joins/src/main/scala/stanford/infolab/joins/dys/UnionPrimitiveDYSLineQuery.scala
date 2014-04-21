/*** JoinPrimitiveDYSLineQuery.scala ***/
package stanford.infolab.joins.dys;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.MutableList

/**
 * Answers a line query with the distributed Yannakakis algorithm using the union primitive:
 * Stores the relations as RDDs containing (Int, Long, Long) tuples. Where the Int stores the ID
 * of the relation. Two relations R_i and R_{i+1} that will be joined or semijoined are unioned
 * first, and then performs the semijoin in the following way. Assume we are performing
 * R_i \semijoin R_{i+1} (so the output will be R_i tuples). We call the method:
 * map-groupByKey-flatMap method: depending on whether each tuple t belongs to R_i or R_{i+1} t is
 * A) mapped with key as its first value or second value; B) The mapped values are then grouped
 * by key; and C) The groups are then searched to see if any tuple is from R_{i+1) and if so output
 * all tuples from R_i, otherwise do not output anything.
 */
class UnionPrimitiveDYSLineQuery extends BaseDYS {

  def parseAndDoSemijoins(sc: SparkContext, dysArgs: DYSArguments):
    ListBuffer[RDD[(Long, Long)]] = {
    doSemiJoins(sc,parseEdgesIntoRDDs(sc, dysArgs.inputFiles))
  }

  private def doSemiJoins(sc: SparkContext, relations: ListBuffer[RDD[(Int, Long, Long)]]):
	  ListBuffer[RDD[(Long, Long)]] = {
    var firstRoundSemiJoinedRelations = doSemiJoinsFromLeftToRight(sc, relations, "SJ1-R");
    val secondRoundSemiJoins = doSemiJoinsFromLeftToRight(sc, firstRoundSemiJoinedRelations.reverse,
      "SJ2-R").reverse
    var finalSemiJoinedRelations = ListBuffer[RDD[(Long, Long)]]();
    for (relation <- secondRoundSemiJoins) {
      val finalSemiJoinedRelation = relation.map(t => (t._2, t._3))
      finalSemiJoinedRelation.setName(relation.name)
      finalSemiJoinedRelations += finalSemiJoinedRelation
    }
    finalSemiJoinedRelations
  }

  private def doSemiJoinsFromLeftToRight(sc: SparkContext, relations:
    ListBuffer[RDD[(Int, Long, Long)]], relationNamePrefix: String):
    ListBuffer[RDD[(Int, Long, Long)]] = {
    println("doSemiJoinsFromLeftToRight called. relations.size: " + relations.size)
    var semiJoinedRelations = ListBuffer[RDD[(Int, Long, Long)]]();

    relations(0).setName(relationNamePrefix + getRelationId(relations(0).name));
    semiJoinedRelations.insert(0, relations(0));
    for (i <- 1 to relations.size - 1) {
      var relationLeft = relations(i)
      var relationRight = semiJoinedRelations(i-1)
      val timeBeforeUnioning = System.currentTimeMillis();
      val unionedRelation = relationLeft.union(relationRight);
      val timeAfterUnioning = System.currentTimeMillis();
      println("Time to union " + relationLeft.name + " and " + relationRight.name +
        ": " + (timeAfterUnioning - timeBeforeUnioning) + "ms.");
      val idLeft = getRelationId(relationLeft.name);
      val idRight = getRelationId(relationRight.name);
      val largerId = Math.max(idLeft, idRight);
      val resultRelationLeft = unionedRelation.map(t => {
          if (t._1 == largerId) {
            (t._2, (t._3, t._1))
          } else {
            (t._3, (t._2, t._1))
          }
        }).groupByKey.flatMap(keyVals => {
          val semiJoinKey = keyVals._1;
          val potentialOutputTuples: MutableList[(Int, Long, Long)] = new MutableList();
          var tupleFromRightRelationExists = false
          for (remainingtuple <- keyVals._2) {
            if (remainingtuple._2 == idRight) {
              tupleFromRightRelationExists = true;
            } else {
              if (idLeft == largerId) {
                potentialOutputTuples += ((idLeft, semiJoinKey, remainingtuple._1))
              } else {
                potentialOutputTuples += ((idLeft, remainingtuple._1, semiJoinKey))
              }
            }
          }
          if (tupleFromRightRelationExists) {
            potentialOutputTuples
          } else {
            List()
          }
        })
      resultRelationLeft.setName(relationNamePrefix + idLeft)
      println("Time to semijoin (by map and groupByKey of union) of " + resultRelationLeft.name +
        ": " + (timeAfterUnioning - timeBeforeUnioning) + "ms.");
      debugRelation(resultRelationLeft);
      semiJoinedRelations.insert(i, resultRelationLeft);
    }
    semiJoinedRelations
  }
  
  private def parseEdgesIntoRDDs(sc: SparkContext, relationFiles: Array[String]):
	  ListBuffer[RDD[(Int, Long, Long)]] = {
    var edgesRDDList = new ListBuffer[RDD[(Int, Long, Long)]]()
    for (i <- 0 to relationFiles.length-1) {
      val edgesRDD = sc.textFile(relationFiles(i), 2).map(
      line => {
        val split = line.split("\\s+")
        (i, split(0).toLong, split(1).toLong)
      })
      edgesRDD.setName("R" + i);
      edgesRDDList += edgesRDD
    }
    edgesRDDList
  }
}