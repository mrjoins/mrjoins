package stanford.infolab.joins.gj

import scala.collection.mutable.ArrayBuffer

/**
 * Wrapper around {@link IntersectionMessage} to keep a message to each partition. Contains
 * convenience functions to handle sending a message to each partition.
 */
class IntersectionMessagesToEachPartition(numPartitions: Short) extends Serializable {
  val intersectionMessages = new ArrayBuffer[IntersectionMessage](numPartitions)
  for (j <- 0 to numPartitions - 1) {
    intersectionMessages += new IntersectionMessage()
  }
  
  def addTuple(nextAttributeVal: Int, tuplesArray: ArrayBuffer[Int], startIndexT: Int, endIndexT: Int,
    startRelationID: Byte) {
    intersectionMessages((nextAttributeVal % numPartitions).toInt).addTuple(tuplesArray, startIndexT,
      endIndexT, startRelationID)
  }
  
  def addTuple(nextAttributeVal: Int, tuplesArray: ArrayBuffer[Int], startIndexT: Int, endIndexT: Int,
    valuesArray: ArrayBuffer[Int], startIndexV: Int, endIndexV: Int, startRelationID: Byte) {
    intersectionMessages((nextAttributeVal % numPartitions).toInt).addTuple(tuplesArray,
      startIndexT, endIndexT, valuesArray, startIndexV, endIndexV, startRelationID)
  }

  def getMessages(): Iterable[(Short, IntersectionMessage)] = {
    val outputs = new ArrayBuffer[(Short, IntersectionMessage)](numPartitions)
    for (partitionID <- 0 to numPartitions - 1) {
      outputs += ((partitionID.toShort, intersectionMessages(partitionID)))
    }
    outputs
  }
  
  override def toString(): String = {
    var retVal = "intersectionMessages:"
    for (i <- 0 to intersectionMessages.length - 1) {
      retVal += "partition " + i + " " + intersectionMessages(i).toString()
    }
    retVal
  }
}
