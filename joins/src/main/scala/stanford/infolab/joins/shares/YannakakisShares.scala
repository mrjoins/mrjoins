package stanford.infolab.joins.shares

import stanford.infolab.joins.JoinsArguments

class YannakakisShares(joinsArgs: JoinsArguments) extends BaseShares(joinsArgs) {

  override def performLocalJoin(arrBuf: Iterable[(Byte, Long, Long)]): Seq[Array[Long]] = {
    null;
  }
}