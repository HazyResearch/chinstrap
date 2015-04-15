package DunceCap

import org.scalatest.FunSuite

class ASTJoinAndSelectTest extends FunSuite {
  test("check that we can rewrite repeated relations in joins properly") {
    val relation1 = ("R", List[String]("a", "b"))
    val relation2 = ("R", List[String]("b", "c"))
    val relation3 = ("R", List[String]("c", "a"))
    val relation4 = ("S", List[String]("c", "d"))
    val relation5 = ("S", List[String]("e", "f"))

    val input = List[dc.JoinedRelation](relation1, relation2, relation3, relation4, relation5)
    val result = ASTJoinAndSelectHelpers.rewriteNamesOfDuplicatedRelations(input)
    assertResult(List(("S",List("_0", "_1")),("R",List("_0", "_1"))))(result._1)
    assertResult(Map(
      ("R1","b") -> ("R",0),
      ("R1","c") -> ("R",1),
      ("R2","a") -> ("R",1),
      ("R0","a") -> ("R",0),
      ("R2","c") -> ("R",0),
      ("R0","b") -> ("R",1),
      ("S0","c") -> ("S",0),
      ("S0","d") -> ("S",1),
      ("S1","e") -> ("S",0),
      ("S1","f") -> ("S",1)))(result._2)
  }
}
