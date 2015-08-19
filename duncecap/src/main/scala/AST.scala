package DunceCap

import scala.collection.mutable

abstract trait ASTNode {
  def code(s: CodeStringBuilder)
  val order = 0
}

case class ASTCreateDB() extends ASTNode {
  override val order = 1
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitInitCreateDB(s)
  }
}

case class ASTLoadRelation(sourcePath:String,rel:Relation) extends ASTNode {
  override val order = 2
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitLoadRelation(s,sourcePath,rel)
  }
}

case class ASTBuildEncodings() extends ASTNode {
  override val order = 3
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitBuildEncodings(s)
  }
}

case class ASTBuildTrie(sourcePath:String,rel:Relation,name:String) extends ASTNode {
  override val order = 4
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitEncodeRelation(s,rel,name)
    CodeGen.emitBuildTrie(s,rel)
  }
}

case class ASTWriteBinaries() extends ASTNode {
  override val order = 100
  override def code(s: CodeStringBuilder): Unit = {
    //write tries
    Environment.relations.foreach( tuple => {
      val (name,rmap) = tuple
      rmap.foreach(tup2 => {
        val (rname,rel) = tup2
          CodeGen.emitWriteBinaryTrie(s,rel)
      })
    })
    //write encodings
    Environment.encodings.foreach(tuple => {
      val (name,encoding) = tuple
      CodeGen.emitWriteBinaryEncoding(s,encoding)
    })
  }
}

class Aggregate(attr:String,agg:String)
class BasicRelation(name:String,attrs:List[String])
class QueryRelation(name:String,attrs:List[(String,String,String)])
class AggregateExpression(attr:String,expression:String)

abstract trait ASTStatement extends ASTNode {}

case class ASTPrintStatement(rel:BasicRelation) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    println("Print relation")
  }
}

//input to this should be 
//(1) list of attrs in the output, 
//(2) list of attrs eliminated (aggregations), 
//(3) list of relations joined
//(4) list of attrs with selections
//(5) list of exressions for aggregations
case class ASTQueryStatement(lhs:BasicRelation,aggs:List[Aggregate],join:List[QueryRelation],aggregateExpressions:List[AggregateExpression]) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    println("Running Query")
    //perform syntax checking
    //run GHD decomp
    //find attr ordering
    //load binaries you need
    //run algorithm
  }
}
