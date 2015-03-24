
import scala.io.Source

object Parser {
  /*
  def main(args: Array[String]) {
    readFile("/dfs/scratch0/caberger/systems/DunceCap/file.txt")
  }*/

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
  }
}
