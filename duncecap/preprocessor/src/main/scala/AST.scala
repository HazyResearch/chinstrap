package DunceCap

import scala.collection.mutable
import sys.process._

abstract trait ASTNode {
  def code(s: CodeStringBuilder)
  val order = 0
}

case class ASTCreateDB() extends ASTNode {
  override val order = 1
  override def code(s: CodeStringBuilder): Unit = {
  }
}

case class ASTLoadRelation(sourcePath:String,rel:Relation) extends ASTNode {
  override val order = 2
  override def code(s: CodeStringBuilder): Unit = {
  }
}

case class ASTBuildEncodings() extends ASTNode {
  override val order = 3
  override def code(s: CodeStringBuilder): Unit = {
  }
}

case class ASTWriteBinaryEncodings() extends ASTNode {
  override val order = 4
  override def code(s: CodeStringBuilder): Unit = {
  }
}

case class ASTEncodeRelation(rel:Relation) extends ASTNode {
  override val order = 5
  override def code(s: CodeStringBuilder): Unit = {
  }
}

case class ASTLoadEncodedRelation(rel:Relation,buildOrder:Int) extends ASTNode {
  override val order = buildOrder
  override def code(s: CodeStringBuilder): Unit = {
  }
}

case class ASTBuildTrie(rel:Relation,name:String,attrs:List[Int],masterName:String,buildOrder:Int) extends ASTNode {
  override val order = buildOrder
  override def code(s: CodeStringBuilder): Unit = {
  }
}

case class ASTWriteBinaryTries() extends ASTNode {
  override val order = 202
  override def code(s: CodeStringBuilder): Unit = {
  }
}

abstract trait ASTStatement extends ASTNode {}

case class ASTPrintStatement(rel:QueryRelation) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
  }
}

//input to this should be 
//(1) list of attrs in the output, 
//(2) list of attrs eliminated (aggregations), 
//(3) list of relations joined
//(4) list of attrs with selections
//(5) list of exressions for aggregations

class ASTLambdaFunction(val inputArgument:QueryRelation,
  val join:List[QueryRelation],
  val aggregates:Map[String,ParsedAggregate])

//change this to lhs; aggregates[attr,(op,expression,init)], join, recursion
case class ASTQueryStatement(
  lhs:QueryRelation,
  joinType:String,
  join:List[QueryRelation],
  recursion:Option[RecursionStatement],
  tc:Option[TransitiveClosureStatement],
  joinAggregates:Map[String,ParsedAggregate] ) extends ASTStatement {
  // TODO (sctu) : ignoring everything except for join, joinAggregates for now

  var minFHWPlans: List[GHD] = createGHD(join);

  def createGHD(rels:List[QueryRelation]): List[GHD] = {
    // We get the candidate GHDs, i.e., the ones of min width
    val candidates = GHDSolver.getMinFHWDecompositions(rels);
    // prefer the GHDs with fewer nodes

    //

    return null;
  }

  override def code(s: CodeStringBuilder): Unit = {

  }
}
