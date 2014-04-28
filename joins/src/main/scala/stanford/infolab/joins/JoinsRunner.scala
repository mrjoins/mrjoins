/*** JoinsRunner.scala ***/
package	stanford.infolab.joins

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.log4j.Logger
import scala.collection.immutable.HashMap
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.SparkListenerTaskEnd
import stanford.infolab.joins.dys.DYS
import stanford.infolab.joins.JoinAlgorithm._
import stanford.infolab.joins.shares.NestedLoopJoinShares

object JoinsRunner {

  /**
   * Answers the line query over m binary relations: R1(A0,A1)R2(A1,A2)...Rm(A(m-1),Am) over
   * a file that contains the edges of a graph.
   */
  def main(args: Array[String]) {
    val joinsArgs = new JoinsArguments(args);
    val environmentMap = HashMap("spark.akka.logLifecycleEvents" -> "true",
      "spark.akka.askTimeout" -> "10",
      "akka.loglevel" -> joinsArgs.logLevel.toString.replace("WARN", "WARNING"),
      "spark.akka.frameSize" -> "1000",
      "spark.serializer" -> "spark.KryoSerializer",
      "spark.kryoserializer.buffer.mb" -> "10",
//      "spark.io.compression.codec" -> "org.apache.spark.io.SnappyCompressionCodec");
      "spark.shuffle.consolidateFiles" -> "true");
    println("running with kryoserializer");
    println("running with shuffle file consolidation");
    println("SPARK_HOME: " + System.getenv("SPARK_HOME"))
    val sc = new SparkContext(joinsArgs.sparkMasterAddr, "MRJoins", System.getenv("SPARK_HOME"),
      joinsArgs.jars, environmentMap, null);
    val listener = new ShuffleReadAndWriteListener(sc);
    sc.addSparkListener(listener)
    
    Logger.getRootLogger.setLevel(joinsArgs.logLevel);

    for (i <- 1 to joinsArgs.numtimesToExecuteQuery) {
      val timeBeforeStartingRound = System.currentTimeMillis();

      var joinsAlgorithm: JoinsAlgorithm = null;
      joinsArgs.joinAlgorithm match {
        case DYS => {
          println("Computing line query with DYS.");
          joinsAlgorithm = new DYS(joinsArgs);
        }
        case SharesNestedLoop => {
          println("Computing line query with NestedLoopJoinShares.");
          joinsAlgorithm = new NestedLoopJoinShares(joinsArgs);
        }
        case SharesYannakakis => {
          throw new RuntimeException("YannakakisShares is not yet supported!");
        }
      }
      val finalJoin = joinsAlgorithm.computeLineQuery(sc);
      if (joinsArgs.outputFile == "") {
        println("Output file not specified. So not saving the results as output file.")
        println("\nFINAL JOIN.  size: " + finalJoin.count() + "\n");
      } else {
        val timeBeforeWriting = System.currentTimeMillis();
        finalJoin.saveAsTextFile(joinsArgs.outputFile);
        println("Total time to write the output: " + (System.currentTimeMillis() - timeBeforeWriting)
          + "ms.");
      }
      val finishTime = System.currentTimeMillis();
      println("Finished Join. Total time: " + (finishTime - timeBeforeStartingRound) + "ms.");
      println("totalShuffleRead: " + listener.totalShuffleRead)
      println("totalShuffleWritte: " + listener.totalShuffleWrite)
    }
    System.exit(0);
  }

  class ShuffleReadAndWriteListener(val sc: SparkContext) extends SparkListener {
    var totalShuffleRead = 0L
    var totalShuffleWrite = 0L

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
      val sid = taskEnd.task.stageId
      val metrics = Option(taskEnd.taskMetrics);
      totalShuffleRead += metrics.flatMap(m => m.shuffleReadMetrics).map(s =>
        s.remoteBytesRead).getOrElse(0L)
      totalShuffleWrite += metrics.flatMap(m => m.shuffleWriteMetrics).map(s =>
        s.shuffleBytesWritten).getOrElse(0L)
    }
  }
}