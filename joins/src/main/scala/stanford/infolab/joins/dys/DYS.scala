/*** DYS.scala ***/
package	stanford.infolab.joins.dys;

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
//import org.apache.spark.SparkConf
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.immutable.HashMap

object DYS {

  /**
   * Answers the line query over m binary relations: R1(A0,A1)R2(A1,A2)...Rm(A(m-1),Am) over
   * a file that contains the edges of a graph.
   */
  def main(args: Array[String]) {
    val dysArgs = new DYSArguments(args);

//    val sparkConf = new SparkConf();
//    sparkConf.setMaster(dysArgs.sparkMasterAddr);
//    sparkConf.setAppName("DYS");
//    if (System.getenv("SPARK_HOME") != null) {
//      sparkConf.setSparkHome(System.getenv("SPARK_HOME"));
//    }
//    if (!dysArgs.jars.isEmpty) {
//      sparkConf.setJars(List("target/scala-2.10/dys_2.10-1.0.jar"));
//    }
//    if (!dysArgs.logLevel.isGreaterOrEqual(Level.WARN)) {
//      sparkConf.set("spark.akka.logLifecycleEvents", "true");
//    }
//    sparkConf.set("spark.akka.askTimeout", "10");
//    sparkConf.set("akka.loglevel", dysArgs.logLevel.toString.replace("WARN", "WARNING"));
//    sparkConf.set("spark.executor.memory", "10g");
//    sparkConf.set("spark.akka.frameSize", "1000");

//    val sc = new SparkContext(sparkConf);
    val environmentMap = HashMap("spark.akka.logLifecycleEvents" -> "true",
      "spark.akka.askTimeout" -> "10",
      "akka.loglevel" -> dysArgs.logLevel.toString.replace("WARN", "WARNING"),
      "spark.executor.memory" -> "10g",
      "spark.akka.frameSize" -> "1000");
    val sc = new SparkContext(dysArgs.sparkMasterAddr, "DYS", System.getenv("SPARK_HOME"),
      dysArgs.jars, environmentMap, null);
    Logger.getRootLogger.setLevel(dysArgs.logLevel);
    if (dysArgs.semijoinAlgorithm == SemijoinAlgorithm.UnionPrimitive) {
        println("Computing line query with UnionPrimitiveDYSLineQuery.");
    	new UnionPrimitiveDYSLineQuery().computeLineQuery(sc, dysArgs);     
    } else {
        println("Computing line query with CogroupPrimitiveDYSLineQuery.");
    	new CogroupPrimitiveDYSLineQuery().computeLineQuery(sc, dysArgs);    
    }
    System.exit(0);
  }
}