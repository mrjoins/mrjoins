package stanford.infolab.joins.gj

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.storage.StorageLevel
import stanford.infolab.joins.JoinsArguments
import stanford.infolab.joins.JoinsAlgorithm
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Partitioner

/**
 * Answers any query with the distributed Generic Join algorithm.
 */
class GenericJoin(joinsArgs: JoinsArguments) extends BaseGenericJoin(joinsArgs) {

  override def computeQuery(sc: SparkContext): RDD[_] = {    
    val startTime = System.currentTimeMillis();
    var relations = if (joinsArgs.isInputInAdjListFormat) parseEdgesIntoRDDFromAdjListFormat(sc)
        else parseEdgesIntoRDDFromEdgeListFormat(sc);
    debugRelation(relations)
    var succ = computeSUCC_A0(relations)
    debugRelation(succ);
    val n = joinsArgs.schemas.flatMap{ a => Seq(a._1, a._2) }.distinct.length;
    println("logLevel: " + joinsArgs.logLevel)
    log.debug("numAttributes: " + n)
    // k is the next attribute we are computing. We compute next succ attribute one at a time.
    for (nextAttribute <- 1 to n-1) {
      log.debug("Computing next succ for attribute: " + nextAttribute)
      succ = computeNextSucc(relations, succ, nextAttribute.toByte);
    }
    succ
  }

  def computeNextSucc(relations: RDD[((Byte, Int), ArrayBuffer[Int])], 
    succ: RDD[(ArrayBuffer[Int], Boolean)], nextAttribute: Byte) : RDD[(ArrayBuffer[Int], Boolean)] = {
    val relConditionAttributeIndices = findRelationConditionAttributeIndex(nextAttribute);
    if (relConditionAttributeIndices.isEmpty) {
      throw new UnsupportedOperationException("Attribute " + nextAttribute + " is not conditioned "
        + " in any of the relations. At least one of the relations need to be conditioned on a "
        + "previous attribute.")
    }
    log.debug("relConditionAttributeIndices: " + relConditionAttributeIndices)
    val countRequests = computeCountRequests(succ, nextAttribute, relConditionAttributeIndices)
    debugRelation(countRequests)
    val countOffers = computeCountOffers(relations, nextAttribute, countRequests)
    debugRelation(countOffers)

    log.debug("countOffer's partitioner: " + countOffers.partitioner.toString());
    log.debug("succ's partitioner: " + succ.partitioner.toString());

    val succCountOffersCogrouped = succ.cogroup(countOffers)
    succCountOffersCogrouped.setName("SUCC_COUNT_OFFERS_COGROUPED_" + nextAttribute)
    debugRelation(succCountOffersCogrouped)

    // tbOffers stands for Tuple-dummyBoolean-countOFFERS
    // IMPORANT TODO(semih): We might have to partition intersections according to some fashion
    // in order to avoid communication during joins with the relations table. If the relations
    // table is shuffled during the joining, then any of the communication advantages are lost.
    // Though by default Spark might be partitioning by the same hash function, so things might
    // work out.
    val numRelsWithCondAttributes = relConditionAttributeIndices.length
    var intersections = succCountOffersCogrouped.flatMap(tbOffers => {
      val tuple: ArrayBuffer[Int] = tbOffers._1
      // TODO(firas): check to see if calling `size' on tbOffers._2._2 (which is an Iterable) is performant
      if (tbOffers._2._2.size < numRelsWithCondAttributes) {
        log.debug("tuple: " + tuple.mkString(" ") + " did not receive enough offers, the " +
          "intersection must be empty.")
        Seq.empty 
      } else {
        // TODO(firas): see if there's something better we can do instead of calling toSeq
        val tbOffersValues = tbOffers._2._2.toSeq
        val plan = tbOffersValues.sortWith((a, b) => a._2 <= b._2).map(a => a._1)
        // POSSIBLE BUG FOR LATER VERSIONS: Since (for now) we can only be conditioning
        // on the first attribute, we directly take a look at the first attribute.
        val attributeVal: Int = tuple(joinsArgs.schemas(plan(0))._1)
        Iterable(((plan(0), attributeVal), (tuple, plan, null: ArrayBuffer[Int])));
      }
    })
    intersections.setName("INTERSECTIONS_FOR_ATTR" + nextAttribute + "_0")
    debugRelation(intersections)
    // TODO(semih): We can run this for loop only numRelsWithCondAttributes - 1 times
    // And in the last time just return the final successful tuples after the final intersection
    for (i <- 1 to numRelsWithCondAttributes) {
      val relationIntersectionsCogrouped = relations.cogroup(intersections);
      relationIntersectionsCogrouped.setName("RELATIONS_INTERSECTIONS_COGROUPED_FOR_ATTR_" 
        + nextAttribute + "_" + i)
      debugRelation(relationIntersectionsCogrouped)
      intersections = relationIntersectionsCogrouped.flatMap(firstattrAdjlistTuplePlanIntersection => {
        if (firstattrAdjlistTuplePlanIntersection._2._2.isEmpty) {
          Seq.empty
        } else if (firstattrAdjlistTuplePlanIntersection._2._1.isEmpty) {
          // During the intersection phase, the adj list for a possible successful tuple is empty
          // It must have been the case that the attribute that was extended in the previous round,
          // was not conditioned on some relation.
          var errorMsg = "During the intersection phase, the adj list for a possible successful " +
              "tuple is empty. Such successors should have been eliminated in the plan" + 
              "construction phase. tuple: " + firstattrAdjlistTuplePlanIntersection._2._2.mkString(" ")
          throw new IllegalStateException(errorMsg)
        } else {
          val firstAttr = firstattrAdjlistTuplePlanIntersection._1._2
          val adjList = firstattrAdjlistTuplePlanIntersection._2._1.head
          val outputs = new ArrayBuffer[((Byte, Int), (ArrayBuffer[Int], Seq[Byte], ArrayBuffer[Int]))]()
          for (tuplePlanIntersection <- firstattrAdjlistTuplePlanIntersection._2._2) {
            val tuple = tuplePlanIntersection._1
            val plan = tuplePlanIntersection._2
            val currentIntersection = tuplePlanIntersection._3
            var intersect: ArrayBuffer[Int] = null;
            if (i == 1) {
              assert(currentIntersection == null, "in the first round of intersection, all "
                + "intersection have to be null")
              if (joinsArgs.countMotifsOnce) {
                intersect = adjList.slice(
                  binarySearchIndexWithMinValueGreaterThanX(adjList, tuple.last), adjList.length)
              } else {
                intersect = adjList
              }
            } else {
              intersect = gallopingIntersect(currentIntersection, adjList)
            }
            // The other two possible outputs are returned here
            if (intersect.length > 0) {
              val relationIndex = if (i == numRelsWithCondAttributes) -1.toByte else plan(i);
              val attributeValue = if (relationIndex == -1) -1 else tuple(joinsArgs.schemas(plan(i))._1)
              outputs += (((relationIndex, attributeValue), (tuple, plan, intersect)))
            }
          }
          outputs
        }
      })
      intersections.setName("INTERSECTIONS_FOR_ATTR_" + nextAttribute + "_" + i)
      debugRelation(intersections)
    }

    // We perform a cartesianproduct of tuple with extensions
    val nextSucc = intersections.flatMap(relIndexAttValTuplePlanIntersect => {
      val tuple = relIndexAttValTuplePlanIntersect._2._1
      val extensions = relIndexAttValTuplePlanIntersect._2._3
      assert(extensions.length > 0, "The extension for each tuple should be non-empty.")
      val outputs = new ArrayBuffer[(ArrayBuffer[Int], Boolean)](extensions.length)
      for (extension <- extensions) {
        outputs += ((tuple :+ extension, true))
      }
      outputs
    })
    nextSucc.setName(succ.name + "_A" + nextAttribute)
    debugRelation(nextSucc)
    nextSucc
  }

  def computeCountOffers(relations: RDD[((Byte, Int), ArrayBuffer[Int])],
    nextAttribute: Byte, countRequests: RDD[((Byte, Int), ArrayBuffer[Int])]):
    RDD[(ArrayBuffer[Int], (Byte, Int))] = {
    val requestsRelationsCogrouped = countRequests.cogroup(relations)
    requestsRelationsCogrouped.setName("COUNT_REQUESTS_RELATIONS_COGROUPED_" + nextAttribute)
    debugRelation(requestsRelationsCogrouped)
    val countOffers = requestsRelationsCogrouped.flatMap(relAttrTuple => {
      val relIndex: Byte = relAttrTuple._1._1
      val conditionedAttr: Int = relAttrTuple._1._2
      if (relAttrTuple._2._2.isEmpty) {
        Seq.empty
      } else {
        var offerSize = if (relAttrTuple._2._2.isEmpty) 0 else relAttrTuple._2._2.head.length
        if (offerSize == 0) {
          Seq.empty
        } else {
          val outputs = new ArrayBuffer[(ArrayBuffer[Int], (Byte, Int))]
          // Relation i only offers counts if its adj list is non-empty
          for (tuple <- relAttrTuple._2._1) {
            if (joinsArgs.countMotifsOnce) {
              offerSize = binarySearchIndexWithMinValueGreaterThanX(relAttrTuple._2._2.head,
                tuple.last);
            }
            outputs += ((tuple, (relIndex, offerSize)))
          }
          outputs
        }
      }
    })
    countOffers.setName("COUNT_OFFERS_" + nextAttribute)
    countOffers
  }

  def computeCountRequests(succ: RDD[(ArrayBuffer[Int], Boolean)], nextAttribute: Byte,
    relAttrIndices: ArrayBuffer[(Byte, Byte)])
     : RDD[((Byte, Int), ArrayBuffer[Int])] = {
    // tb stands for tupleBoolean
    val countRequests = succ.flatMap(tb => {
      val tuple = tb._1
      log.debug("tuple: " + tuple(0))
      val outputs = new ArrayBuffer[((Byte, Int), ArrayBuffer[Int])]()
      for (relAttrIndex <- relAttrIndices) {
        log.debug("relAttrIndex._2: " + relAttrIndex._2)
        outputs += (((relAttrIndex._1, tuple(relAttrIndex._2)), tuple))
      }
      outputs
    })
    countRequests.setName("COUNT_REQUESTS_" + nextAttribute)
    countRequests
  }  

  def computeSUCC_A0(relations: RDD[((Byte, Int), ArrayBuffer[Int])])
     : RDD[(ArrayBuffer[Int], Boolean)] = {
    val a1Indices = findIndices(0)
    
    // We transform each tuple of the relations that contain attribute A1 into
    // (A1-attr, relation-index)
    val firstAttributes = relations.flatMap(u => {
      if (a1Indices.contains(u._1._1)) Seq((u._1._2, u._1._1))
      else None
    })

    val a1IndicesLength = a1Indices.length
    // attrRelations is of form: (first-attr, rel-index)
    // Output if of form (first-attr, dummy-boolean) if there are k rel-indices for an attribute
    // where k is the number of relations that contain attribute A1
    // Note(semih): As far as I can tell I need a dummy boolean so that in further iterations
    // we can join SUCC with INTERSECTION (the successful next attributes for each tuple).
    val groupedFirstAttributes = firstAttributes.groupByKey();
    groupedFirstAttributes.setName("GROUPED_FIRST_ATTRIBUTE")
    debugRelation(groupedFirstAttributes);
    val succ = groupedFirstAttributes.flatMap(attrRelations => 
      // TODO(firas): same as before -- check to make sure calling `size' here is okay
      if (attrRelations._2.size == a1IndicesLength) Seq((ArrayBuffer(attrRelations._1), false))
      else None)
    succ.setName("SUCC_A0")
    succ
  }

  def parseEdgesIntoRDDFromEdgeListFormat(sc: SparkContext): RDD[((Byte, Int), ArrayBuffer[Int])] = {
    val timeBeforeParsing = System.currentTimeMillis();
    var edgesRDDList = new ListBuffer[RDD[(Int, Int)]]()
    var relations: RDD[((Byte, Int), ArrayBuffer[Int])] = null
    for (i <- 0 to joinsArgs.inputFiles.length-1) {
      val edgesRDD = sc.textFile(joinsArgs.inputFiles(i), joinsArgs.mapParallelism).map(
      line => {
        val split = line.split("\\s+")
        ((i.toByte, split(0).toInt), split(1).toInt)
      })
      val edgesRDDInAdjListFormat: RDD[((Byte, Int), ArrayBuffer[Int])] =
        edgesRDD.groupByKey().map(value => {
          val adjList = new ArrayBuffer[Int]();
          // TODO(firas): same as before -- see if there's something better we can do besides converting to Seq
          adjList.appendAll(value._2.toSeq.sortWith(_ < _))
          (value._1, adjList)
        })

      if (relations == null) {
        relations = edgesRDDInAdjListFormat
      } else {
        relations ++= edgesRDDInAdjListFormat
      }
    }
    relations.setName("RELATIONS")
    relations
  }

  /**
   * Parses a file in adjacency list format.
   * Returns a single RDD of form ((rel-index, first attribute), adjlist).
   * Warning Note: The adjlist is sorted in increasing order during parsing!
   */
  def parseEdgesIntoRDDFromAdjListFormat(sc: SparkContext): RDD[((Byte, Int), ArrayBuffer[Int])] = {
    val timeBeforeParsing = System.currentTimeMillis()
    var relations: RDD[((Byte, Int), ArrayBuffer[Int])] = null
    for (i <- 0 to joinsArgs.inputFiles.length-1) {
      val edgesRDD = sc.textFile(joinsArgs.inputFiles(i), joinsArgs.mapParallelism).map(
      line => {
        val split = line.split("\\s+")
        var adjList = new ArrayBuffer[Int](split.length - 1)
        for (j<-1 to split.length - 1) {
          adjList += split(j).toInt
        }
        ((i.toByte, split(0).toInt), adjList.sortWith(_ < _))
      })
      if (relations == null) {
        relations = edgesRDD
      } else {
        relations ++= edgesRDD
      }
    }
    relations.setName("RELATIONS")
    relations
  }
}