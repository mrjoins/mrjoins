package stanford.infolab.joins.shares

import stanford.infolab.joins.JoinsArguments

class YannakakisShares(joinsArgs: JoinsArguments) extends BaseShares(joinsArgs) {

  def performLocalJoin(arrBuf: Seq[(Byte, Long, Long)]): Seq[Array[Long]] = {
    val mapByRelationId = arrBuf.groupBy(arr => arr._1)
    
//    var semiJoinedEdgesList = semijoin(semiJoinedEdgesList);
    
    var result: Seq[Array[Long]] = Seq.empty
    if (mapByRelationId.keySet.size == joinsArgs.m) {
      // tuples and otherTuples are ArrayBuffers
      val tuples = mapByRelationId(0).map(x => Array(x._2, x._3))
      val otherTuples = mapByRelationId(1).map(x => Array(x._2, x._3))

      result =
        for (tuple <- tuples; otherTuple <- otherTuples; if (tuple.last == otherTuple.head)) yield {
          tuple ++ otherTuple.tail
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

//  def semijoin(edgeLists: ListBuffer[RDD[(Long, Long)]]):
//    ListBuffer[RDD[(Long, Long)]] = {
//    val semijoinedRelations = doSemiJoins(edgeLists)
//    if (joinsArgs.cacheIntermediateResults) {
//      for (semijoinedRelation <- semijoinedRelations) {
//        semijoinedRelation.persist(StorageLevel.MEMORY_ONLY_SER);
//      }
//    }
//    semijoinedRelations;
//  }
}