/**
 * Command-line parser for JoinsRunner.
 */
package stanford.infolab.joins

import org.apache.log4j.Level
import scala.Array.canBuildFrom

class JoinsArguments(args: Array[String]) extends Serializable {
  var joinAlgorithm: JoinAlgorithm.Value = null;
  var sparkMasterAddr: String = "";
  var inputFiles: Array[String] = Array.empty[String];
  var outputFile: String = ""
  var m: Int = -1
  var logLevel = Level.WARN
  var jars: Array[String] = Array.empty[String];
  var mapParallelism: Int = 10;
  var reduceParallelism: Int = 10;
  var numPartitions: Short = 10; // num partitions to use for memory optimized columnar storages
  var cacheIntermediateResults: Boolean = false;
  var unpersistIntermediateResults: Boolean = false;
  var skipSemijoining: Boolean = false;
  var numtimesToExecuteQuery = 1;
  var kryoCompression: Boolean = true;
  var kryoBufferSizeMB: Int = 10;
  var schemas: Array[(Byte, Byte)] = Array.empty[(Byte, Byte)];
  var isInputInAdjListFormat: Boolean = true;
  var numCores: Int = -1;
  var countMotifsOnce: Boolean = false;

  parse(args.toList)

  if (inputFiles.length == 1 & m <= 1) {
    println("If you specify one relation as inputFiles, then you have to specify -m > 1")
    printUsageAndExit(1)
  } else if (inputFiles.length > 1 && m > 1 && (m != inputFiles.length)) {
    println("If you specify multiple relations as inputFiles and specify m, then m has to equal" +
      "inputFiles.length")
    printUsageAndExit(1)
  }

  if (inputFiles.length == 1) {
    for (i <- 2 to m) {
      inputFiles = inputFiles :+ inputFiles(0)
    }
  } else {
    m = inputFiles.length;
  }

  def parse(args: List[String]): Unit = args match {
    case ("--algorithm" | "-alg") :: value :: tail =>
      if (value == "dys") {
        joinAlgorithm = JoinAlgorithm.DYS;
      } else if (value == "sharesNested") {
        joinAlgorithm = JoinAlgorithm.NestedLoopJoinShares;
      } else if (value == "sharesYannakakis") {
        joinAlgorithm = JoinAlgorithm.YannakakisShares;
      } else if (value == "gj") {
        joinAlgorithm = JoinAlgorithm.GenericJoin;
      } else if (value == "mogj") {
        joinAlgorithm = JoinAlgorithm.MemoryOptimizedGenericJoin;
      } else {
        throw new RuntimeException("Value of the algorithm (--algorithm or -alg argument) has to" +
          " be one of {dys/sharesNested/sharesYannakakis}");
      }
	  if (!tail.isEmpty) parse(tail)

	  // The schema of each relation is as follows: R_k=(A_i, A_j) where i < j. This should be
	  // represented in the specified argument, e.g: 1,2::2,4::2,3::3,4
	  case ("--schema" | "-sch") :: value :: tail =>
      val schemasStringSplit = value.split("::")
	    schemas = new Array[(Byte, Byte)](schemasStringSplit.length)
      for (i <- 0 to schemasStringSplit.length - 1) {
        val schema = schemasStringSplit(i).split("-");
        schemas(i) = (schema(0).toByte, schema(1).toByte);
      }
	    if (!tail.isEmpty) parse(tail)

    case ("--sparkMaster" | "-sm") :: value :: tail =>
      sparkMasterAddr = value
	  if (!tail.isEmpty) parse(tail)

    case ("--inputFiles" | "-ifs") :: value :: tail =>
      inputFiles = value.split("::")
	  if (!tail.isEmpty) parse(tail)

    case ("--inputFormat" | "-iformat") :: value :: tail =>
      if (value == "adjlist") isInputInAdjListFormat = true
      else if (value == "edgelist") isInputInAdjListFormat = false
      else {
        throw new RuntimeException("The format of the input files can be either adjlist or " 
          + "edgelist. current value is: " + value);}
      if (!tail.isEmpty) parse(tail)

    case ("--outputFile" | "-of") :: value :: tail =>
      outputFile = value
	  if (!tail.isEmpty) parse(tail)

    case ("--numRelations" | "-m") :: value :: tail =>
      m = value.toInt
	  if (!tail.isEmpty) parse(tail)

	case ("--jars" | "-j") :: value :: tail =>
      jars = value.split("::")
	  if (!tail.isEmpty) parse(tail)

	case ("--mapParallelism" | "-mp") :: value :: tail =>
      mapParallelism = value.toInt
	  if (!tail.isEmpty) parse(tail)

	case ("--reduceParallelism" | "-rp") :: value :: tail =>
      reduceParallelism = value.toInt
	  if (!tail.isEmpty) parse(tail)

	case ("--numPartitions" | "-np") :: value :: tail =>
      numPartitions = value.toShort
    if (!tail.isEmpty) parse(tail)

  case ("--numCores" | "-nc") :: value :: tail =>
      numCores = value.toInt
    if (!tail.isEmpty) parse(tail)

	case ("--cacheIntermediateResults" | "-cir") :: value :: tail =>
      cacheIntermediateResults = value.toBoolean
	  if (!tail.isEmpty) parse(tail)

	case ("--unpersist" | "-unpersist") :: value :: tail =>
      unpersistIntermediateResults = value.toBoolean
	  if (!tail.isEmpty) parse(tail)

	case ("--skipSemijoining" | "-ssj") :: value :: tail =>
      skipSemijoining = value.toBoolean
	  if (!tail.isEmpty) parse(tail)

	case ("--numTimesToExecuteQuery" | "-nteq") :: value :: tail =>
      numtimesToExecuteQuery = value.toInt
	  if (!tail.isEmpty) parse(tail)

	case ("--kryoCompression" | "-kryo") :: value :: tail =>
      kryoCompression = value.toBoolean
	  if (!tail.isEmpty) parse(tail)

	case ("--kryoBufferSizeMB" | "-kbs") :: value :: tail =>
      kryoBufferSizeMB = value.toInt
	  if (!tail.isEmpty) parse(tail)

	case ("--countMotifsOnce" | "-cmo") :: value :: tail =>
    countMotifsOnce = value.toBoolean
    if (!tail.isEmpty) parse(tail)
	  
	case ("--verbose" | "-v") :: tail =>
      logLevel = Level.DEBUG
	  if (!tail.isEmpty) parse(tail)
    
  case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case _ =>
      println("" + args)
      printUsageAndExit(1)
  }

  /**
   * Print usage of DYS and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    val usage =
      """
        |Usage: DYS [options] 
        |Usually ran through sbt run [options]
        |
        |***Currently only implements line queries***
        |Options:
        |   -alg {dys/sharesNested/sharesYannakakis}, --algorithm {dys/sharesNested/sharesYannakakis} Which distributed join algorithm to run
        |   -ifs file1::file2::..., --inputFiles file1::file2::... 	Full path to the input relation files separated by ::
        |   -iformat adj/edge, --inputFormat adj/edge The format of the input files. Can either be adjlist or edgelist.
        |   -m numRelations, --numRelations numRelations 			Number of relations to join. Should be specified only if only one input file is specified.
        |   -sm sparkMasterAddr, --sparkMaster sparkMasterAddr 		Address of the spark master (e.g. local, spark://iln01.stanford.edu:7077)
        |   -of outputFile, --outputFile outputFile                 Full path to the output file to store the output of the join
        |   -j jarFiles, --jars jarFiles							List of jar files that should be passed to SparkContext
        |   -mp parallelism, --mapParallelism parallelism			Level of parallelism in `mapping` stages. Essentially sets the number of partitions
        |															of intial RDDs.
        |   -rp parallelism, --reduceParallelism parallism			Level of parallelism in `reducing` stages. Essentially sets the number of partitions
        |															of cogrouped/joined RDDs.
        |   -np #partitions, --numPartitions #partitions Number of partitions to use when using the memory optimized columnar storage for joins.
        |   -nc #cores, --numCores #cores number of cores to use in the cluster when executing the algorithms. Essentially sets the spark.cores.max parameter
        |   -cir true/false, --cacheIntermediateResults true/false  Whether to cache intermediate results during the join.
        |   -unpersist true/false, --unpersist true/false			Whether to unpersist cached intermediate results when possible
        |   -ssj true/false, --skipSemijoining true/false			Whether to skip the semijoining phase and directly join the tables (as Shark does)
        |   -nteq [# times], --numTimesToExecuteQuery [# times]	    Number of times to execute query.
        |   -kryo {true/false}, --kryoCompression {true/false}      Whether to use kryo compression.
        |   -kbs bufferSizeMB, --kryoBufferSizeMB                   Kryo buffer size in MB (10 by default)
        |   -cmo {true/false}, --countMotifsOnce {true/false}  Whether to count triangles/rectangles once.
        |   -v, --verbose                  							Print more debugging output
      
      """.stripMargin
    System.err.println(usage)
    System.exit(exitCode)
  }
}

object JoinAlgorithm extends Enumeration {
  type JoinAlgorithm = Value
  val DYS, NestedLoopJoinShares, SortedNestedLoopJoinShares, YannakakisShares, GenericJoin, MemoryOptimizedGenericJoin = Value
}
