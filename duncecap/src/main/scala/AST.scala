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

class Aggregate(val attr:String,val agg:String)
class SelectionRelation(val name:String,val attrs:List[(String,String,String)])
class AggregateExpression(val attr:String,val expression:String)
class SelectionCondition(val condition:String,val value:String)
class QueryRelation(val name:String,val attrs:List[String]) {
  override def equals(that: Any): Boolean =
    that match {
      case that: QueryRelation => that.attrs.equals(attrs) && that.name.equals(name)
      case _ => false
    }
}

abstract trait ASTStatement extends ASTNode {}

case class ASTPrintStatement(rel:QueryRelation) extends ASTStatement {
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
case class ASTQueryStatement(lhs:QueryRelation,aggs:List[Aggregate],join:List[SelectionRelation],aggregateExpressions:List[AggregateExpression]) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    //perform syntax checking (TODO)

    //run GHD decomp
    println(lhs.name + " " + lhs.attrs)
    join.foreach(qr =>
      println(qr.name + " " +  qr.attrs)
    )
    val relations = join.map(qr => new QueryRelation(qr.name,qr.attrs.map{_._1}))
    val selections = join.flatMap(qr => qr.attrs.filter(atup => atup._2 != "").map(atup => (atup._1,new SelectionCondition(atup._2,atup._3)) ) ).toMap
    val myghd = GHDSolver.getGHD(relations) //get minimum GHD's

    //find attr ordering
    val attributeOrdering = GHDSolver.getAttributeOrdering(myghd,lhs.attrs)
    println("GLOBAL ATTR ORDER: " + attributeOrdering)
    
    //Figure out the relations and encodings you need for the query
    val reorderedRelations = relations.map(qr => {
      val reorderedAttrs = qr.attrs.sortBy(attributeOrdering.indexOf(_))
      val rName = qr.name + "_" + reorderedAttrs.map(qr.attrs.indexOf(_)).mkString("")
      assert(Environment.relations.contains(qr.name))
      assert(Environment.relations(qr.name).contains(rName))
      ((qr.name,new QueryRelation(rName,reorderedAttrs)))
    })
    val encodings = reorderedRelations.flatMap(r => Environment.relations(r._1).head._2.encodings ).toList.distinct

    reorderedRelations.foreach(tup => {
      val (name,qr) = tup
      println(qr.name + " " +  qr.attrs)
    })
    println("Encodings: " + encodings)


    //load binaries you need
    CodeGen.emitLoadBinaryEncoding(s,encodings)
    CodeGen.emitLoadBinaryRelation(s,reorderedRelations.map(_._2.name).distinct)

    //run algorithm
  }
}
