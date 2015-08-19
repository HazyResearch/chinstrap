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
object QueryFileReader {
  def readFile(file:String): String = {
    val source = scala.io.Source.fromFile(file)
    val line = try source.mkString finally source.close()
    line
  }
}
object Repl {
  def run : Unit = {
    while(true){
      print(">")
      val line = readLine()
      if(line == "exit" || line == "quit")
        System.exit(0)
      val codeStringBuilder = new CodeStringBuilder
      DCParser.run(line,codeStringBuilder)
    
      Environment.clearASTNodes()
    }
  }
}

object QueryCompiler extends App {
  val usage = "Usage: ./QueryCompiler <path to config file> <optional query file otherwise repl starts>"
  if (args.length != 1 && args.length != 2) {
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
  Utils.loadEnvironmentFromJSON(config,false)

  if(args.length == 2){ //reading a query from a file
    val codeStringBuilder = new CodeStringBuilder
    DCParser.run(QueryFileReader.readFile(args(1)),codeStringBuilder) 
  } else {
    Repl.run
  }
}
