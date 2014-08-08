package stanford.infolab.joins.gj

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.Logging

/**
 * Represents a message sent from one partition to another during the intersection phase.
 * 
 * An IntersectionMessage contains the following:
 *  <ul>
 *    <li> SuccTuples: An ArrayBuffer[Int] representing tuples. If each succ tuple has x
 *    attributes, then the 1st x indices contain the attributes of the 1st tuple consecutively,
 *    values between x + 1 and 2x contain the attributes of the 2nd tuple, etc.
 *    <li> Intersections: An ArrayBuffer[Int]. For each tuple contains the latest intersection
 *    among the relations. If the 1st tuple has y1 elements in its latest intersection, then the
 *    first y1 elements contain the 1st tuple's current latest successful intersection. Similarly
 *    if the 2n tuple currently has y2 elements in its latest intersection, the elements between
 *    y1+1 and y1+y2 contain that set, etc.
 *    <li> Offsets: An ArrayBuffer[Int]. Containing the offsets in the intersections array of the
 *    intersection set of each tuple in SuccTuples
 *    <li> StartRelationID: An ArrayBuffer[Byte], containing the ID of the first relation that the
 *    tuple started intersecting from. This information is enough to infer which is the next
 *    attribute and relation to condition things on.
 *  </ul>
 */
class IntersectionMessage() extends Logging with Serializable {
  val succTuples = new ArrayBuffer[Int]()
  val intersections = new ArrayBuffer[Int]()
  val offsets = new ArrayBuffer[Int]()
  val startRelationIDs = new ArrayBuffer[Byte]()
  
  def addTuple(tuplesArray: ArrayBuffer[Int], startIndexT: Int, endIndexT: Int,
    startRelationID: Byte) {
    addTuple(tuplesArray, startIndexT, endIndexT, null, -1, -1, startRelationID)
  }

  def addTuple(tuplesArray: ArrayBuffer[Int], startIndexT: Int, endIndexT: Int,
    valuesArray: ArrayBuffer[Int], startIndexV: Int, endIndexV: Int, startRelationID: Byte) {
    log.debug("startIndex: " + startIndexT + " endIndex: " + endIndexT)
    for (j <- startIndexT to endIndexT - 1) {
      log.debug("adding tuple element: " + tuplesArray(j))
      succTuples += tuplesArray(j)
    }

    if (valuesArray != null) {
      for (j <- startIndexV to endIndexV - 1) {
        log.debug("adding value element: " + valuesArray(j))
        intersections += valuesArray(j)
      }
      if (offsets.isEmpty) {
        offsets += 0
      }
      offsets += offsets.last + (endIndexV - startIndexV)
    }
    startRelationIDs += startRelationID

  }

  override def toString(): String = {
    var retVal = "succTuples: [" + succTuples.mkString(" ") + "]";
    retVal += "\nintersections: [" + intersections.mkString(" ") + "]"
    retVal += "\noffsets: [" + offsets.mkString(" ") + "]"
    retVal += "\nstartRelationIDs: [" + startRelationIDs.mkString(" ") + "]"
    retVal
  }
}
