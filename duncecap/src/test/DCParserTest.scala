import DunceCap.{ASTRelation, ASTLoadExpression, DCParser}
import org.scalatest.FunSuite

class DCParserTest extends FunSuite {

  test("Strings get parsed correctly") {
    val tabThenNewlineThenChars = DCParser.parseAll(DCParser.string, "\"\\t\\na\\sd\\fghjkl\"")
    assert(tabThenNewlineThenChars.successful)
    assertResult("\t\nasdfghjkl")(tabThenNewlineThenChars.get)

    val escapedBackslashBeforeEndQuote = DCParser.parseAll(DCParser.string, "\"foo\\\\\"")
    assert(escapedBackslashBeforeEndQuote.successful)
    assertResult("foo\\")(escapedBackslashBeforeEndQuote.get)
  }

  test("Can correctly parse a simple load statement") {
    val result1 = DCParser.parseAll(DCParser.loadExpr, "R(a:long) <- load(\"filename\", tsv)")
    assert(result1.successful)
    assertResult(new ASTLoadExpression(new ASTRelation(Map(("a", "long"))),"filename","tsv"))(result1.get)

    val result2 = DCParser.parseAll(DCParser.loadExpr, "R2(a:float,b:long, c:string) <- load (\"fil\\\"ename\",csv  )")
    assert(result2.successful)
    assertResult(new ASTLoadExpression(new ASTRelation(Map(("c", "string"), ("b", "long"), ("a", "float"))),"fil\"ename","csv"))(result2.get)
  }

  test("Rejects malformed load statements") {
    val unrecognizedFormat = DCParser.parseAll(DCParser.loadExpr, "R(a:long,b:long) <- load(\"filename\", unrecognizedFormat)")
    assert(!unrecognizedFormat.successful)

    val unrecognizedType = DCParser.parseAll(DCParser.loadExpr, "R(a:long,b:unrecognizedType) <- load(\"filename\", tsv)")
    assert(!unrecognizedType.successful)

    val noSchema = DCParser.parseAll(DCParser.loadExpr, "R() <- load(\"filename\", tsv)")
    assert(!noSchema.successful)
  }
}
