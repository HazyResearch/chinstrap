import DunceCap.DCParser
import org.scalatest.FunSuite

class DCParserTest extends FunSuite {
  test("Can correctly parse a simple load statement") {
    val parser : DCParser = new DCParser
    parser.parseAll(parser.loadExpr, "R(a:long,b:long) <- load(\"filename\", \"tsv\")") match {
      case parser.Success(lup, _) => println(lup)
      case x => println(x)
    }

    parser.parseAll(parser.loadExpr, "R(a:long,b:long,c:string) <-load( \"file\\\"name\", \"tsv\")") match {
      case parser.Success(lup, _) => println(lup)
      case x => println(x)
    }
  }
}
