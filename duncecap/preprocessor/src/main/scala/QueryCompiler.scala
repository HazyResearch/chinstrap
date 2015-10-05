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
    Environment.quiet = true
    while(true){
      print(">")
      val line = readLine()
      if(line == "exit" || line == "quit")
        System.exit(0)
      val codeStringBuilder = new CodeStringBuilder
      try {
        DCParser.run(line,codeStringBuilder)
        Utils.compileAndRun(codeStringBuilder,"repl_program")
      } catch {
        case e:Throwable => println("Exception: " + e)
      }
      Environment.clearASTNodes()
    }
  }
}

object QueryCompiler extends App {
  val usage = "Usage: ./QueryCompiler <query file>"
  println(args)
  if(args.length == 1){ //reading a query from a file
    val codeStringBuilder = new CodeStringBuilder
    DCParser.run(QueryFileReader.readFile(args(0)),codeStringBuilder)
   // Utils.compileAndRun(codeStringBuilder,args(0).split('/').last.split('.').toList.head)
  } else {
    println(usage)
    System.exit(1)
  }
}
