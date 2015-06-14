package DunceCap

import java.io.{FileWriter, BufferedWriter, File}

object GHDWriter {
  def main(args: Array[String]): Unit = {
    //triangle()
    //tadpole3_1()
    //tadpole4_1()
    //fourClique()
    /*
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
    */
    val barbell: List[Relation] = List(
      new Relation(List("a", "b"),"R"),
      new Relation(List("b", "c"),"S"),
      new Relation(List("c", "a"),"T"),
      new Relation(List("a2", "b2"),"A"),
      new Relation(List("b2", "c2"),"B"),
      new Relation(List("c2", "a2"),"C"),
      new Relation(List("a", "a2"),"BRIDGE"))
    printAllMinWidthDecomps(barbell, "barbell")
    val lubm: List[Relation] = List(
      new Relation(List("a", "b"),"R"),
      new Relation(List("b", "c"),"S"),
      new Relation(List("c", "a"),"T"),
      new Relation(List("a", "a2"),"A2"),
      new Relation(List("b", "b2"),"B2"),
      new Relation(List("c", "c2"),"C2"))
    printAllMinWidthDecomps(lubm, "lubm")
  }

  def printAllMinWidthDecomps(rels : List[Relation], name : String): Unit = {
    val solver = GHDSolver
    val decompositions = solver.getMinFHWDecompositions(rels)
    val fhws = decompositions.map{(root : GHDNode) => 
      println(root.fractionalScoreTree())
      root.rels.foreach( r => println("Name: " + r.name))
      root.fractionalScoreTree()
    }
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zipWithIndex.map({case (root, index) => solver.print(root, "/dfs/scratch0/caberger/systems/chinstrap/query_plans/gen/" + name +"_generated" + index + ".json")})
  }

  def tadpole3_1(): Unit = {
    val solver = GHDSolver
    val TADPOLE: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "a")),
      new Relation(List("a", "e")))
    val decompositions = solver.getDecompositions(TADPOLE)
    val fhws = decompositions.map((root : GHDNode) => root.fractionalScoreTree())
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zip(fhws).map({case (root, fhw) => solver.print(root, "/dfs/scratch0/caberger/systems/chinstrap/query_plans/gen/tadpole3_1_generated" + fhw + ".json")})
  }

  def tadpole4_1(): Unit = {
    val solver = GHDSolver
    val TADPOLE: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "a")),
      new Relation(List("a", "e")))
    val decompositions = solver.getDecompositions(TADPOLE)
    val fhws = decompositions.map((root : GHDNode) => root.fractionalScoreTree())
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zip(fhws).zipWithIndex.map({case (plan_info, index) => solver.print(plan_info._1, "/dfs/scratch0/caberger/systems/chinstrap/query_plans/gen/lollipop3_1_generated" + plan_info._2 + "_" + index + ".json")})
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
    val decompositions = solver.getDecompositions(CLIQUE)
    val fhws = decompositions.map((root : GHDNode) => root.fractionalScoreTree())
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zip(fhws).map({case (root, fhw) => solver.print(root, "/dfs/scratch0/caberger/systems/chinstrap/query_plans/gen/clique4_generated" + fhw + ".json")})
  }

  def triangle(): Unit = {
    val solver = GHDSolver
    val TRI: List[Relation] = List(
      new Relation(List("a", "b")),
      new Relation(List("b", "c")),
      new Relation(List("c", "a")))
    val decompositions = solver.getDecompositions(TRI)
    val fhws = decompositions.map((root : GHDNode) => root.fractionalScoreTree())
    decompositions.map((root : GHDNode) => root.reorderAttributes())
    decompositions.zip(fhws).map({case (root, fhw) => solver.print(root, "/dfs/scratch0/caberger/systems/chinstrap/query_plans/gen/triangle_generated" + fhw + ".json")})
  }
}