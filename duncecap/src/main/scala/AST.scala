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

case class ASTWriteBinaryEncodings() extends ASTNode {
  override val order = 4
  override def code(s: CodeStringBuilder): Unit = {
    //write encodings
    Environment.encodings.foreach(tuple => {
      val (name,encoding) = tuple
      CodeGen.emitWriteBinaryEncoding(s,encoding)
    })
  }
}

case class ASTEncodeRelation(rel:Relation) extends ASTNode {
  override val order = 5
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitEncodeRelation(s,rel)
  }
}

case class ASTLoadEncodedRelation(rel:Relation) extends ASTNode {
  override val order = 200
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitLoadEncodedRelation(s,rel)
  }
}

case class ASTBuildTrie(rel:Relation,name:String,attrs:List[Int],masterName:String) extends ASTNode {
  override val order = 201
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitReorderEncodedRelation(s,rel,name,attrs,masterName)
    CodeGen.emitBuildTrie(s,rel,masterName)
  }
}

case class ASTWriteBinaryTries() extends ASTNode {
  override val order = 202
  override def code(s: CodeStringBuilder): Unit = {
    //write tries
    Environment.relations.foreach( tuple => {
      val (name,rmap) = tuple
      rmap.foreach(tup2 => {
        val (rname,rel) = tup2
          CodeGen.emitWriteBinaryTrie(s,name,rel.name)
      })
    })
  }
}

class SelectionRelation(val name:String,val attrs:List[(String,String,String)])
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
    CodeGen.emitPrintRelation(s,rel)
  }
}

//input to this should be 
//(1) list of attrs in the output, 
//(2) list of attrs eliminated (aggregations), 
//(3) list of relations joined
//(4) list of attrs with selections
//(5) list of exressions for aggregations
case class ASTQueryStatement(lhs:QueryRelation,aggregates:Map[String,String],join:List[SelectionRelation],aggregateExpressions:Map[String,String]) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    //perform syntax checking (TODO)

    //first emit allocators
    CodeGen.emitAllocators(s)

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

    println("AGGREGATES: " + aggregates)
    
    //map each attribute to an encoding and type
    val attrToEncodingMap = relations.flatMap(qr => {
      val rName = qr.name + "_" + (0 until qr.attrs.length).mkString("_")
      (0 until qr.attrs.length).map(i => {
        (qr.attrs(i) -> (Environment.relations(qr.name)(rName).encodings(i),Environment.relations(qr.name)(rName).types(i)))
      }).toList.distinct
    }).toMap
    println("ATTR TO ENCODING: " + attrToEncodingMap)

    //Figure out the relations and encodings you need for the query
    val reorderedRelations = relations.map(qr => {
      val reorderedAttrs = qr.attrs.sortBy(attributeOrdering.indexOf(_))
      val rName = qr.name + "_" + reorderedAttrs.map(qr.attrs.indexOf(_)).mkString("_")
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
    CodeGen.emitLoadBinaryEncodings(s,encodings)
    CodeGen.emitLoadBinaryRelations(s,reorderedRelations.map(r => (r._1,r._2.name) ).distinct)

    //if the number of bags is 1 or the attributes in the root match those in the result
    val topDownUnecessary = myghd .num_bags == 1 ||  
      (lhs.attrs.intersect(myghd.attrSet.toList).length == lhs.attrs.length)
    println("topDownUnecessary: " + topDownUnecessary)

    val lhsOrder = (0 until lhs.attrs.length).filter(i => attributeOrdering.contains(lhs.attrs(i))).sortBy(i => attributeOrdering.indexOf(lhs.attrs(i))).toList
    val scalarResult = lhsOrder.length == 0

    val aggregateExpression = if(aggregateExpressions.isEmpty) "" else aggregateExpressions.head._2
    val aggregateOrder = aggregates.toList.map(_._1).sortBy(attributeOrdering.indexOf(_))

    val annotatedAttr = 
      if(aggregateOrder.length != 0 && !scalarResult && lhsOrder.length != 0) 
        Some(lhs.attrs(lhsOrder(lhsOrder.length-1)))
      else 
        None

    //run algorithm
    GHDSolver.bottomUp(myghd, ((ghd:GHDNode) => {
      val attrOrder = ghd.attrSet.toList.sortBy(attributeOrdering.indexOf(_))
      val lhsAttrs = lhs.attrs.filter(attrOrder.contains(_)).sortBy(attrOrder.indexOf(_))
      val name = myghd.getName(attrOrder)
      val bagLHS = new QueryRelation("bag_" + name,lhsAttrs)

      //figure out if there are selections below the last materialized attribute
      val lhsLastIndex = if(lhsAttrs.length != 0) attrOrder.indexOf(lhsAttrs.last) else attrOrder.length
      var selectionAfterLast = if(selections.size != 0) 
        selections.map(op => {attrOrder.indexOf(op._1) > lhsLastIndex}).reduce((a,b) => a || b)
        else 
          false

      val codeGenAttrs = attrOrder.map(a => {
        //create aggregate
        val materialize = lhsAttrs.contains(a)
        val first = a == attrOrder.head 
        val last = a == attrOrder.last
        val prev = if(!first) Some(attrOrder(attrOrder.indexOf(a)-1)) else None
        val aggregate = if(aggregates.contains(a)) Some(aggregates(a)) else None
        val selection = if(selections.contains(a)) Some(selections(a)) else None
        val materializeViaSelectionsBelow = if(selectionAfterLast && a == lhsAttrs.last) true else false
        val checkSelectionNotMaterialize = if(selectionAfterLast && attrOrder.indexOf(a) > attrOrder.indexOf(lhsAttrs.last)) true else false
        println(a + " MM: " + materializeViaSelectionsBelow)
        val accessors = reorderedRelations.
          filter(rr => rr._2.attrs.contains(a)).
          map(rr => new Accessor(rr._2.name,rr._2.attrs.indexOf(a),rr._2.attrs) ).
          groupBy(a => a.getName()).map(_._2.head).toList
        new CodeGenNPRRAttr(a,aggregate,accessors,selection,materialize,first,last,prev,materializeViaSelectionsBelow,checkSelectionNotMaterialize)
      })
      CodeGen.emitNPRR(s,name,new CodeGenGHD(bagLHS,codeGenAttrs,aggregateExpression,scalarResult,aggregateOrder,annotatedAttr,attrToEncodingMap))
    }))
    
    val lhsEncodings = lhsOrder.map(i => attrToEncodingMap(lhs.attrs(i))._1).toList
    val lhsTypes = lhsOrder.map(i => attrToEncodingMap(lhs.attrs(i))._2).toList
    val annotations = (lhs.attrs.filter(!attributeOrdering.contains(_)).mkString("_"))
    val lhsName = lhs.name+"_"+lhsOrder.mkString("_")
    if(!topDownUnecessary){
      s.println("//top down code")
    } else{
      //emit the actual trie name you want
      val attrOrder = myghd.attrSet.toList.sortBy(attributeOrdering.indexOf(_))
      val name = myghd.getName(attrOrder)
      CodeGen.emitRewriteOutputTrie(s,lhsName,"bag_" + name,scalarResult)
    }

    if(!scalarResult){
      //below here should probably be refactored. this saves the environment and writes the trie to disk
      Environment.addRelation(lhs.name,new Relation(lhsName,lhsTypes,lhsEncodings))
      Utils.writeEnvironmentToJSON()

      s"""mkdir -p ${Environment.dbPath}/relations/${lhs.name} ${Environment.dbPath}/relations/${lhs.name}/${lhsName}""" !
      
      CodeGen.emitWriteBinaryTrie(s,lhs.name,lhsName)
    }
  }
}
