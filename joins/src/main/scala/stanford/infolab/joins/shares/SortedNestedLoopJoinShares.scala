package stanford.infolab.joins.shares

import stanford.infolab.joins.JoinsArguments
import java.util.Arrays
import scala.collection.mutable.MutableList
import scala.collection.mutable.HashMap

class SortedNestedLoopJoin(joinsArgs: JoinsArguments) extends BaseShares(joinsArgs) {

  def performLocalJoin(arrBuf: Seq[(Byte, Long, Long)]): Seq[Array[Long]] = {
    var latest: Seq[Array[Long]] = Seq.empty
    val mapByRelationId = new HashMap[Byte, MutableList[Array[Long]]]() { override def default(key: Byte) = MutableList() }

    for (tuple <- arrBuf) {
      mapByRelationId(tuple._1) = mapByRelationId(tuple._1) += Array(tuple._2, tuple._3)
    }
    if (mapByRelationId.keySet.size == joinsArgs.m) {
      latest = mapByRelationId(0).sortBy(x => x.last)
      for (i <- 1 until joinsArgs.m) {
        val joinResult = MutableList[Array[Long]]()
        val rel = mapByRelationId(i.toByte).sortBy(x => x.head)

        var j = 0; var k = 0;
        while (j < latest.size && k < rel.size) {
          val leftTuple = latest(j); val rightTuple = rel(k);
          if (leftTuple.last == rightTuple.head) {
            joinResult += (leftTuple ++ rightTuple.tail)
            k += 1
          } else if (leftTuple.last < rightTuple.head) {
            j += 1
          } else { // leftTuple.last > rightTuple.head
            k += 1
          }
        }
        latest = joinResult.sortBy(x => x.last)
      }
    }
    latest
  }
}