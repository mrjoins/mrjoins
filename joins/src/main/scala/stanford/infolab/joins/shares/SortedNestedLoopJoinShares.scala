package stanford.infolab.joins.shares

import stanford.infolab.joins.JoinsArguments

class SortedNestedLoopJoin(joinsArgs: JoinsArguments) extends BaseShares(joinsArgs) {

  //TODO()    
  override def performLocalJoin(arrBuf: Iterable[(Byte, Long, Long)]): Seq[Array[Long]] = {
    null
  }
}