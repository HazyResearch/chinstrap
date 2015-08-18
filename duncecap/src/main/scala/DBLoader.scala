package DunceCap

import scala.util.parsing.json._
import scala.io._

/**
 * You can run the output of this class by doing
 *
 * ./bin/outputFilename
 *
 * from the emptyheaded/bin directory
 */
object DBLoader extends App {
  println("DBLoader")
  if (args.length != 1){
    val usage = "Usage: ./DBLoader <path to JSON config file> "
    println(usage)
    System.exit(1)
  } 
  val filename = args(0)
  val fileContents = Source.fromFile(filename).getLines.mkString
  val config:Map[String,Any] = JSON.parseFull(fileContents) match {
    case Some(map: Map[String, Any]) => map
    case _ => Map()
  }
  
  //load JSON file (creates directory structure for DB as well)
  Environment.addASTNode(ASTCreateDB())
  Environment.addASTNode(ASTBuildEncodings())
  Environment.addASTNode(ASTWriteBinaries())
  Utils.loadEnvironmentFromJSON(config,true)

  //emit code
  val codeStringBuilder = new CodeStringBuilder
  Environment.emitASTNodes(codeStringBuilder) //builds the relations & encodings

  //compile and run C++ code
  Utils.compileAndRun(codeStringBuilder)
}