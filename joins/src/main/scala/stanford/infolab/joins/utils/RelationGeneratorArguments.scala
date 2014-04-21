/**
 * Command-line parser for RelationGenerator.
 */
package stanford.infolab.joins.utils;

import org.apache.log4j.Level

class RelationGeneratorArguments(args: Array[String]) {
  var numTuples: Int = -1;
  var domainSizes: List[Long] = List.empty[Long];
  var outputFile: String = "";

  parse(args.toList)

  if (numTuples <= 0) { 
    println("number of tuples need to be positive")
    printUsageAndExit(1)
  }

  if (domainSizes.isEmpty) { 
    println("domain sizes are not specified")
    printUsageAndExit(1)
  }
  
  def parse(args: List[String]): Unit = args match {
    case ("--numTuples" | "-k") :: value :: tail =>
      numTuples = value.toInt
	  if (!tail.isEmpty) parse(tail)

	case ("--domainSizes" | "-ds") :: value :: tail =>
	  for (size <-  value.split("::")) {
        domainSizes = domainSizes :+ size.toLong;
      }
	  if (!tail.isEmpty) parse(tail)

    case ("--outputFile" | "-of") :: value :: tail =>
      outputFile = value
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
        |Usage: RelationGenerator [options] 
        |Usually ran through sbt run [options]
        |
        |***Currently only generates relations with long/int values stored in a text file***
        |Options:
        |   -k numTuples, --numTuples numTuples   				Number of tuples in the relation 
        |   -ds domainSizesStr, --domainSizes domainSizesString A string of the format s1::s2::...::sn,
      	|														where s_i is the size of the domain for
      	|														attribute i and n is the number of
      	|														attributes of the relation.
        |   -of outputFile, --ouputFile outputFile				Full path to the output file 
      """.stripMargin
    System.err.println(usage)
    System.exit(exitCode)
  }
}
