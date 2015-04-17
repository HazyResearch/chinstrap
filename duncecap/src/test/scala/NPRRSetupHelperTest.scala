import DunceCap._
import org.scalatest.FunSuite
import NPRRSetupHelper._

class NPRRSetupHelperTest extends FunSuite {
  test("Check that we can build encoding sets") {
    val relations = List[Relation](
      ("R", List("a", "b", "c")),
      ("R", List("b", "d", "c")),
      ("S", List("c", "d")),
      ("S", List("e", "f")))
    val equivalenceClasses = buildEncodingEquivalenceClasses(relations)
    assertResult(Set[List[(RName, RIndex)]](
      List(("R", 0), ("R", 1), ("S", 1)),
      List(("R", 2), ("S", 0))
    ))(equivalenceClasses)
  }
}
