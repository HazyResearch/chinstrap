import DunceCap._
import org.scalatest.FunSuite
import NPRRSetupUtil._

class NPRRSetupUtilTest extends FunSuite {
  private  val relations = List[Relation](
    ("R", List("a", "b", "c")),
    ("R", List("b", "d", "c")),
    ("S", List("c", "d")),
    ("S", List("e", "f")))

  test("Check that we can find distinct relations") {
    assertResult(List[RWRelation](
      ("S", List(0,1)),
      ("R",List(0, 1, 2))))(getDistinctRelations(relations))
  }

  test("Check that we can build encoding sets and finding encodings relevant to attrs") {
    val equivalenceClasses = buildEncodingEquivalenceClasses(relations)
    assertResult(List[List[(RName, RIndex)]](
      List(("R", 0), ("R", 1), ("S", 1)),
      List(("R", 2), ("S", 0))
    ))(equivalenceClasses)

    val a_encodings  = getEncodingsRelevantToAttr("a", relations, equivalenceClasses)
    assertResult(Set[Column](("R", 0)))(a_encodings)
    val b_encodings  = getEncodingsRelevantToAttr("b", relations, equivalenceClasses)
    assertResult(Set[Column](("R", 0)))(b_encodings)
    val c_encodings  = getEncodingsRelevantToAttr("c", relations, equivalenceClasses)
    assertResult(Set[Column](("R", 2)))(c_encodings)
    val d_encodings  = getEncodingsRelevantToAttr("d", relations, equivalenceClasses)
    assertResult(Set[Column](("R", 0)))(d_encodings)
    val e_encodings  = getEncodingsRelevantToAttr("e", relations, equivalenceClasses)
    assertResult(Set[Column](("R", 2)))(e_encodings)
    val f_encodings  = getEncodingsRelevantToAttr("f", relations, equivalenceClasses)
    assertResult(Set[Column](("R", 0)))(f_encodings)
  }
}
