package stanford.infolab.joins.gj

import org.apache.spark.Partitioner
import java.util.Arrays
import org.apache.spark.HashPartitioner
import scala.collection.mutable.ArrayBuffer

class ModuloPartitionIDPartitioner(partitions: Int) extends Partitioner {
  def numPartitions = partitions
  
  override def getPartition(key: Any): Int = key match {
    case shortKey: Short => {
      println("Inside ModuloPartitionIDPartitioner. numPartitions: " + numPartitions) 
      shortKey % numPartitions
    }
    case _ => throw new IllegalArgumentException("Keys to LongArrayPartitioner has to be of type Array[Long]")
  }

  override def hashCode: Int = numPartitions
  
  override def toString(): String = { "LongArrayPartitioner with numPatitions: " + numPartitions }
}