import DunceCap.{ASTRelation, ASTLoadExpression, DCParser}
import org.scalatest.FunSuite

class DCParserTest extends FunSuite {
  final val parser : DCParser = new DCParser

  test("Strings get parsed correctly") {
    val tabThenNewlineThenChars = parser.parseAll(parser.string, "\"\\t\\na\\sd\\fghjkl\"")
    assert(tabThenNewlineThenChars.successful)
    assertResult("\t\nasdfghjkl")(tabThenNewlineThenChars.get)

    val escapedBackslashBeforeEndQuote = parser.parseAll(parser.string, "\"foo\\\\\"")
    assert(escapedBackslashBeforeEndQuote.successful)
    assertResult("foo\\")(escapedBackslashBeforeEndQuote.get)
  }

  test("Can correctly parse a simple load statement") {
    val result1 = parser.parseAll(parser.loadExpr, "R(a:long) <- load(\"filename\", tsv)")
    assert(result1.successful)
    assertResult(new ASTLoadExpression(new ASTRelation(Map(("a", "long"))),"filename","tsv"))(result1.get)

    val result2 = parser.parseAll(parser.loadExpr, "R2(a:float,b:long, c:string) <- load (\"fil\\\"ename\",csv  )")
    assert(result2.successful)
    assertResult(new ASTLoadExpression(new ASTRelation(Map(("c", "string"), ("b", "long"), ("a", "float"))),"fil\"ename","csv"))(result2.get)
  }

  test("Rejects malformed load statements") {
    val unrecognizedFormat = parser.parseAll(parser.loadExpr, "R(a:long,b:long) <- load(\"filename\", unrecognizedFormat)")
    assert(!unrecognizedFormat.successful)

    val unrecognizedType = parser.parseAll(parser.loadExpr, "R(a:long,b:unrecognizedType) <- load(\"filename\", tsv)")
    assert(!unrecognizedType.successful)

    val noSchema = parser.parseAll(parser.loadExpr, "R() <- load(\"filename\", tsv)")
    assert(!noSchema.successful)
  }
}
