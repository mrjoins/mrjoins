package stanford.infolab.joins.shares

import stanford.infolab.joins.JoinsArguments
import scala.collection.mutable.ArrayBuffer

class NestedLoopJoinShares(joinsArgs: JoinsArguments) extends BaseShares(joinsArgs) {

  override def performLocalJoin(arrBuf: Iterable[(Byte, Long, Long)]): Seq[Array[Long]] = {
    val mapByRelationId = arrBuf.groupBy(
      arr => arr._1)
    
    // TODO(firas): switch from ArrayBuffer to something more compact
    var result: ArrayBuffer[Array[Long]] = new ArrayBuffer[Array[Long]]() 
    if (mapByRelationId.keySet.size == joinsArgs.m) {
      // tuples and otherTuples are ArrayBuffers
      val tuples = mapByRelationId(0).map(x => Array(x._2, x._3))
      val otherTuples = mapByRelationId(1).map(x => Array(x._2, x._3))

      for (tuple <- tuples) {
        for (otherTuple <- otherTuples) {
          if (tuple.last == otherTuple.head) {
            result += (tuple ++ otherTuple.tail)
          }
        }
      }

      for (i <- 2 until joinsArgs.m) {
        val moreTuples = mapByRelationId(i.toByte).map(x => Array(x._2, x._3))
        result =
          for (tuple <- result; otherTuple <- moreTuples; if (tuple.last == otherTuple.head)) yield {
            tuple ++ otherTuple.tail
          }
      }
    }
    result
  }
}