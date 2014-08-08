package stanford.infolab.joins.gj

import stanford.infolab.joins.JoinsArguments
import scala.collection.mutable.ArrayBuffer
import stanford.infolab.joins.JoinsAlgorithm

abstract class BaseGenericJoin(joinsArgs: JoinsArguments) extends JoinsAlgorithm(joinsArgs) {

  def findIndices(relationIndex: Byte): List[Byte] = {
    var indices: List[Byte] = List();
    for (i <- 0 to joinsArgs.schemas.length - 1) {
      val schema = joinsArgs.schemas(i);
     if (schema._1 == relationIndex || schema._2 == relationIndex) {
       indices = indices :+ i.toByte;
      } 
    }
    return indices;
  }

  // POTENTIAL BUG FOR LATER VERSIONS: For now we assume that for each tuple t of SUCC, when we
  // extend t, each relation can be conditioned on exactly one attribute. This condition is
  // trivially satisfied when each relation is binary but may not be satisfied in general.
  def findRelationConditionAttributeIndex(nextAttribute: Byte): ArrayBuffer[(Byte, Byte)] = {
    val relAttrIndices = new ArrayBuffer[(Byte, Byte)]()
    for (relIndex <- 0 to joinsArgs.schemas.length - 1) {
      val schema = joinsArgs.schemas(relIndex.toByte);
      if (schema._2 == nextAttribute) {
        relAttrIndices += ((relIndex.toByte, schema._1))
      }
    }
    relAttrIndices
  }
  
  def binarySearchIndexWithMinValueGreaterThanX(array: ArrayBuffer[Int], value: Int): Int = {
    binarySearchIndexWithMinValueGreaterThanX(array, 0, array.length - 1, value)
  }
  
  def binarySearchIndexWithMinValueGreaterThanX(array: ArrayBuffer[Int], low: Int, high: Int,
    value: Int): Int = {
    var l = low; 
    var h = high;
    var m = -1;
    while (l <= h) {
      m = (h + l)/2
      if (array(m) < value) l = m+1
      else if (array(m) > value) h = m-1
      else return m+1
    }
    l
  }
  
  // Acknowledgement: Copied from Frank McSherry's C# implementation 
  def gallopingIntersect(currentIntersection: ArrayBuffer[Int], adjList: ArrayBuffer[Int]): ArrayBuffer[Int] = {
    var buffer = adjList;
    var cursor = 0
    var extent = adjList.length
    var array = currentIntersection;
    val intersect = new ArrayBuffer[Int]();
    for (value <- array) {
      var comparison = buffer(cursor) - value;

      if (comparison < 0) {
        var step = 1;
        while (cursor + step < extent && buffer(cursor + step) < value) {
          cursor = cursor + step;
          step = step << 1;
        }
        step = step >> 1;
        // either cursor + step is off the end of the array, or points at a value larger than otherValue
        while (step > 0) {
          if (cursor + step < extent) {
            if (buffer(cursor + step) < value)
              cursor = cursor + step;
          }
          step = step >> 1;
        }
        cursor += 1;
        if (cursor >= extent)
          return intersect;
        comparison = buffer(cursor) - value;
      }
      if (comparison == 0 && cursor < extent) {
        intersect += value;
        cursor += 1;
        if (cursor >= extent)
          return intersect;
      }
    }
    return intersect;
  }

      // Acknowledgement: Copied from Frank McSherry's C# implementation 
  // Difference from above is that instead of returning an array, it modifies the elements
  // of current intersection. Elements of in the successful intersection are stored in
  // the prefix of currentIntersection. It returns the total number of successful intersections.
  // NOTE: curIntEndIndex, and adjListEndIndex are not considered in the search
  def gallopingIntersect2(currentIntersection: ArrayBuffer[Int], curIntBeginIndex: Int,
    curIntEndIndex: Int, adjList: ArrayBuffer[Int], adjListBeginIndex: Int, adjListEndIndex: Int)
    :Int = {
    var buffer = adjList;
    var cursor = adjListBeginIndex
    var extent = adjListEndIndex
    var array = currentIntersection;
    var counts = 0;
    var value = -1
    log.debug("curIntBeginIndex: " + curIntBeginIndex + " curIntEndIndex: " + curIntEndIndex)
    for (i <- curIntBeginIndex to curIntEndIndex - 1) {
      value = array(i)
      var comparison = buffer(cursor) - value;

      if (comparison < 0) {
        var step = 1;
        while (cursor + step < extent && buffer(cursor + step) < value) {
          cursor = cursor + step;
          step = step << 1;
        }
        step = step >> 1;
        // either cursor + step is off the end of the array, or points at a value larger than otherValue
        while (step > 0) {
          if (cursor + step < extent) {
            if (buffer(cursor + step) < value)
              cursor = cursor + step;
          }
          step = step >> 1;
        }
        cursor += 1;
        if (cursor >= extent)
          return counts;
        comparison = buffer(cursor) - value;
      }
      if (comparison == 0 && cursor < extent) {
        array(curIntBeginIndex + counts) = value;
        counts += 1
        cursor += 1;
        if (cursor >= extent)
          return counts;
      }
    }
    counts
  }
}