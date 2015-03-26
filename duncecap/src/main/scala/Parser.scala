package DunceCap

import scala.io.Source
import scala.util.matching.Regex

object Parser {
  def readFile(filename:String){
    println("Parsing file: " + filename)
    try {
      for (line <- Source.fromFile(filename).getLines()) {
        parseLine(line)
      }
    } catch {
      case ex: Exception => println("ERROR: Reading file.")
    }
  }

  def parseLine(line:String){
    println(line)
    
    val expr = new Expression(line)

    throw new IllegalStateException("Exception thrown");
  }
}