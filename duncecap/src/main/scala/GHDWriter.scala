package DunceCap

import java.io.{FileWriter, BufferedWriter, File}

object GHDWriter {
  def main(args: Array[String]): Unit = {
    //triangle()
    //tadpole3_1()
    // tadpole4_1()
    //fourClique()

    val freebase: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "d")),
      new Relation(List("d", "e")),
      new Relation(List("a", "e")),
      new Relation(List("e", "f")),
      new Relation(List("f", "g")),
      new Relation(List("g", "h")),
      new Relation(List("h", "a")))
    printAllMinWidthDecomps(freebase, "freebase")
    val q7: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c", "d")),
      new Relation(List("b", "e")),
      new Relation(List("a", "c")))
    printAllMinWidthDecomps(q7, "q7")
    val barbell: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "a")),
      new Relation(List("a2", "b2")),
      new Relation(List("b2", "c2")),
      new Relation(List("c2", "a2")),
      new Relation(List("a", "a2")))
    printAllMinWidthDecomps(barbell, "barbell")
  }

  def printAllMinWidthDecomps(rels : List[Relation], name : String): Unit = {
    case class Precision(val p:Double)
    class withAlmostEquals(d:Double) {
      def ~=(d2:Double)(implicit p:Precision) = (d-d2).abs <= p.p
    }
    implicit def add_~=(d:Double) = new withAlmostEquals(d)
    implicit val precision = Precision(0.001)

    val solver = GHDSolver
    val decompositions = solver.getMinFractionalWidthDecomposition(rels)
    val fhws = decompositions.map((root : GHDNode) => (root.fractionalScoreTree(), root))
    val minScore = fhws.unzip._1.min
    println(s"""minScore: ${minScore}""")
    val minFhws = fhws.filter((scoreAndNode : (Double, GHDNode)) => scoreAndNode._1 ~= minScore)
    minFhws.map({case (fhw, root)=> root.reorderAttributes()})
    minFhws.unzip._2.zipWithIndex.map({case (root, index) => print(root, "../query_plans/gen/" + name +"_generated" + index + ".json")})
  }

  def tadpole3_1(): Unit = {
    val solver = GHDSolver
    val TADPOLE: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "a")),
      new Relation(List("a", "e")))
    val decompositions = solver.getMinFractionalWidthDecomposition(TADPOLE)
    val fhws = decompositions.map((root : GHDNode) => root.fractionalScoreTree())
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zip(fhws).map({case (root, fhw) => print(root, "../query_plans/gen/tadpole3_1_generated" + fhw + ".json")})
  }

  def tadpole4_1(): Unit = {
    val solver = GHDSolver
    val TADPOLE: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "a")),
      new Relation(List("a", "e")))
    val decompositions = solver.getMinFractionalWidthDecomposition(TADPOLE)
    val fhws = decompositions.map((root : GHDNode) => root.fractionalScoreTree())
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zip(fhws).zipWithIndex.map({case (plan_info, index) => print(plan_info._1, "../query_plans/gen/lollipop3_1_generated" + plan_info._2 + "_" + index + ".json")})
  }

  def fourClique(): Unit = {
    val solver = GHDSolver
    val CLIQUE: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "a")),
      new Relation(List("b", "d")),
      new Relation(List("a", "d")),
      new Relation(List("d", "c")))
    val decompositions = solver.getMinFractionalWidthDecomposition(CLIQUE)
    val fhws = decompositions.map((root : GHDNode) => root.fractionalScoreTree())
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zip(fhws).map({case (root, fhw) => print(root, "../query_plans/gen/clique4_generated" + fhw + ".json")})
  }

  def triangle(): Unit = {
    val solver = GHDSolver
    val TRI: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "a")))
    val decompositions = solver.getMinFractionalWidthDecomposition(TRI)
    val fhws = decompositions.map((root : GHDNode) => root.fractionalScoreTree())
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zip(fhws).map({case (root, fhw) => print(root, "../query_plans/gen/triangle_generated" + fhw + ".json")})
  }

  def print(root: GHDNode, filename: String) = {
    val json = root.toJson()
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.append(json.spaces2)
    bw.close()
  }
}