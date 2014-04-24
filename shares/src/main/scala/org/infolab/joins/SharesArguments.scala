package org.infolab.joins

import org.apache.log4j.Level

class SharesArguments(args: Array[String]) {
  var sparkMasterAddr: String = "";
  var inputFiles: Array[String] = Array.empty[String];
  var outputFile: String = ""
  var m: Int = -1
  var semijoinAlgorithm = SemijoinAlgorithm.CogroupPrimitive;
  var logLevel = Level.WARN
  var jars: Array[String] = Array.empty[String];
  
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
    case ("--sparkMaster" | "-sm") :: value :: tail =>
      sparkMasterAddr = value
	  if (!tail.isEmpty) parse(tail)

    case ("--inputFiles" | "-ifs") :: value :: tail =>
      inputFiles = value.split("::")
	  if (!tail.isEmpty) parse(tail)

    case ("--outputFile" | "-of") :: value :: tail =>
      outputFile = value
	  if (!tail.isEmpty) parse(tail)

    case ("--numRelations" | "-m") :: value :: tail =>
      m = value.toInt
	  if (!tail.isEmpty) parse(tail)

    case ("--semijoinAlg" | "-sja") :: value :: tail =>
      if (value == "cogroup") {
        semijoinAlgorithm = SemijoinAlgorithm.CogroupPrimitive;
      } else if (value == "union") {
        semijoinAlgorithm = SemijoinAlgorithm.UnionPrimitive;
      }
	  if (!tail.isEmpty) parse(tail)

	case ("--jars" | "-j") :: value :: tail =>
      jars = value.split("::")
	  if (!tail.isEmpty) parse(tail)

	case ("--verbose" | "-v") :: tail =>
      logLevel = Level.INFO
	  if (!tail.isEmpty) parse(tail)
    
    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case _ =>
      println("" + args)
      printUsageAndExit(1)
  }

  /**
   * Print usage of Shares and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    val usage =
      """
        |Usage: Shares [options] 
        |Usually ran through sbt run [options]
        |
        |***Currently only implements line queries***
        |Options:
        |   -ifs file1::file2::..., --inputFiles file1::file2::... 	Full path to the input relation files separated by ::
        |   -m numRelations, --numRelations numRelations 			Number of relations to join. Should be specified only if only one input file is specified.
        |   -sm sparkMasterAddr, --sparkMaster sparkMasterAddr 		Address of the spark master (e.g. local, spark://iln01.stanford.edu:7077)
        |   -sja {cogroup, union}, --semijoinAlg {cogroup, union} 	Type of the semijoin algorithm: either joins two 
        |                                                        	relations by join-map to give back the left one,
        |                                                        	or unions them first and does: map-groupBy-flatMap 
        |                                                        	(see different implementations for detail)
        |   -of outputFile, --outputFile outputFile                 Full path to the output file to store the output of the join
        |   -j jarFiles, --jars jarFiles							List of jar files that should be passed to SparkContext
        |   -v, --verbose                  							Print more debugging output
      
      """.stripMargin
    System.err.println(usage)
    System.exit(exitCode)
  }  
}

object SemijoinAlgorithm extends Enumeration {
  val CogroupPrimitive, UnionPrimitive = Value
}
