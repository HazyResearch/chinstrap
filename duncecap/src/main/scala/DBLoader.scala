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
  val db_folder = config("database").asInstanceOf[String]

  //load JSON file (creates directory structure for DB as well)
  Environment.addASTNode(ASTCreateDB())
  Environment.addASTNode(ASTBuildEncodings())
  Environment.addASTNode(ASTWriteBinaryEncodings())

  //Environment.addASTNode(ASTWriteBinaryTries())
  Utils.loadEnvironmentFromJSON(config,true,db_folder)

  //emit code
  Utils.writeEnvironmentToJSON()

  //compile and run C++ code
  val codeStringBuilder1 = new CodeStringBuilder
  val work1 = Environment.astNodes.toList.sortBy(astN => astN.order).filter(astN => astN.order < 200)
  CodeGen.emitCode(codeStringBuilder1,work1)
  Utils.compileAndRun(codeStringBuilder1,"load_" + Environment.dbPath.split("/").toList.last + "_1")

  val codeStringBuilder2 = new CodeStringBuilder
  val work2 = Environment.astNodes.toList.sortBy(astN => astN.order).filter(astN => astN.order >= 200)
  CodeGen.emitCode(codeStringBuilder2,work2)
  Utils.compileAndRun(codeStringBuilder2,"load_" + Environment.dbPath.split("/").toList.last + "_2")

}