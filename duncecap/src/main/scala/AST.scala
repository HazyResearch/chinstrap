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

case class ASTLoadEncodedRelation(rel:Relation,buildOrder:Int) extends ASTNode {
  override val order = buildOrder
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitLoadEncodedRelation(s,rel)
  }
}

case class ASTBuildTrie(rel:Relation,name:String,attrs:List[Int],masterName:String,buildOrder:Int) extends ASTNode {
  override val order = buildOrder
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
    val relations = join.map(qr => new QueryRelation(qr.name,qr.attrs.map{_._1}))
    val selections = join.flatMap(qr => qr.attrs.filter(atup => atup._2 != "").map(atup => (atup._1,new SelectionCondition(atup._2,atup._3)) ) ).toMap

    val root = GHDSolver.getGHD(relations) //get minimum GHD's

    //find attr ordering
    val attributeOrdering = GHDSolver.getAttributeOrdering(root,lhs.attrs)
    
    if(Environment.debug){
      println("NUM BAGS: " + root.getNumBags())
      println("GLOBAL ATTR ORDER: " + attributeOrdering)
    }
    
    //map each attribute to an encoding and type
    val attrToEncodingMap = relations.flatMap(qr => {
      val rName = qr.name + "_" + (0 until qr.attrs.length).mkString("_")
      (0 until qr.attrs.length).map(i => {
        (qr.attrs(i) -> (Environment.relations(qr.name)(rName).encodings(i),Environment.relations(qr.name)(rName).types(i)))
      }).toList.distinct
    }).toMap

    //Figure out the relations and encodings you need for the query
    val reorderedRelations = relations.map(qr => {
      val reorderedAttrs = qr.attrs.sortBy(attributeOrdering.indexOf(_))
      val rName = qr.name + "_" + reorderedAttrs.map(qr.attrs.indexOf(_)).mkString("_")
      assert(Environment.relations.contains(qr.name))
      assert(Environment.relations(qr.name).contains(rName))
      ((qr.name,new QueryRelation(rName,reorderedAttrs)))
    })
    val encodings = reorderedRelations.flatMap(r => Environment.relations(r._1).head._2.encodings ).toList.distinct

    //load binaries you need
    CodeGen.emitLoadBinaryEncodings(s,encodings)
    CodeGen.emitLoadBinaryRelations(s,reorderedRelations.map(r => (r._1,r._2.name) ).distinct)
    CodeGen.emitStartQueryTimer(s)

    //if the number of bags is 1 or the attributes in the root match those in the result
    //we intersect the lhs attrs with the attribute ordering so the aggregations are eliminated
    val topDownUnecessary = root.getNumBags() == 1 ||  
      (lhs.attrs.intersect(root.attrSet.toList).length == lhs.attrs.intersect(attributeOrdering).length)

    val lhsOrder = (0 until lhs.attrs.length).filter(i => attributeOrdering.contains(lhs.attrs(i))).sortBy(i => attributeOrdering.indexOf(lhs.attrs(i))).toList
    val lhsName = lhs.name+"_"+lhsOrder.mkString("_")
    val scalarResult = lhsOrder.length == 0

    val aggregateExpression = if(aggregateExpressions.isEmpty) "" else aggregateExpressions.head._2

    //run algorithm
    val seenNodes = mutable.ListBuffer[CodeGenGHDNode]()
    val seenGHDNodes = mutable.ListBuffer[(GHDNode,List[String])]()

    GHDSolver.bottomUp(root, ((ghd:GHDNode,isRoot:Boolean,parent:GHDNode) => {
      val attrOrder = ghd.attrSet.toList.sortBy(attributeOrdering.indexOf(_))
      val lhsAttrs = lhs.attrs.filter(attrOrder.contains(_)).sortBy(attrOrder.indexOf(_))
      val parentAttrs = if(!isRoot) parent.attrSet.toList.sortBy(attrOrder.indexOf(_)) else List()
      val sharedAttrs = attrOrder.intersect(parentAttrs).sortBy(attrOrder.indexOf(_))
      val materializedAttrs = lhsAttrs.union(sharedAttrs).distinct.sortBy(attributeOrdering.indexOf(_))

      val name = ghd.getName(attrOrder)
      val bagLHS = new QueryRelation("bag_" + name,lhsAttrs)

      //FIX ME. For a relation to be in a bag right now we require all its attributes are in the bag
      val bagRelations = reorderedRelations.filter(rr => {
        rr._2.attrs.intersect(attrOrder).length != rr._2.attrs.length
      })

      //any attr that is shared in the children 
      //map from the attribute to a list of accessors
      val childrenAttrMap = ghd.children.flatMap(cn => {
        val childAttrs = cn.attrSet.toList.sortBy(attributeOrdering.indexOf(_))
        val childName = cn.getName(childAttrs)
        childAttrs.toList.intersect(attrOrder).map(a => {
          (a,new Accessor("bag_"+childName,childAttrs.indexOf(a),childAttrs)) 
        })
      }).groupBy(t => t._1).map(t => (t._1 -> (t._2.map(_._2).distinct)) )

      //a list with the attributes that are aggregated in order
      val aggregateOrder = aggregates.toList.map(_._1).sortBy(attributeOrdering.indexOf(_)).filter(a => {
        !sharedAttrs.contains(a) && attrOrder.contains(a) //must be in current bag and not shared to survive
      })

      val bagScalarResult = (materializedAttrs.length == 0) && (aggregates.size != 0)
      val cgenGHDNode = new CodeGenGHDNode(bagLHS,bagScalarResult,attrToEncodingMap)
      var noWork = false
      val seenAccessors = mutable.ListBuffer[String]() //don't duplicate accessors
      attrOrder.foreach(a => {
        val selection = if(selections.contains(a)) Some(selections(a)) else None
        val materialize = materializedAttrs.contains(a)
        val prevMaterialized = if(materializedAttrs.indexOf(a) < 1) None else Some(materializedAttrs(materializedAttrs.indexOf(a)-1))  //prev should be the previous materialized attribute
        val lastMaterialized = materializedAttrs.indexOf(a) == (materializedAttrs.length-1)

        //access each of the relations in the bag that contain the attribute
        val attributeAccessors = bagRelations.
          filter(rr => rr._2.attrs.contains(a)). //get the accessors the match this attr
          map(rr => new Accessor(rr._2.name,rr._2.attrs.indexOf(a),rr._2.attrs) ). 
          groupBy(a => a.getName()).map(_._2.head).toList //perform a distinct operation on the accessors
        
        //those shared in the children
        val passedAccessors = if(childrenAttrMap.contains(a)) childrenAttrMap(a) else List[Accessor]()
        val accessors = (attributeAccessors ++ passedAccessors).filter(acc => !seenAccessors.contains(acc.getName()))
        seenAccessors ++= accessors.map(_.getName())

        val selectionBelow = (attrOrder.indexOf(a) until attrOrder.length).map(i => selections.contains(attrOrder(i))).reduce((a,b) => a || b)
        val annotated = lastMaterialized && (attrOrder.indexOf(a) until attrOrder.length).map(i => aggregates.contains(attrOrder(i))).reduce((a,b) => a || b)
        val annotation = if(aggregates.contains(a)) {
          val passedAnnotations = if(childrenAttrMap.contains(a)) childrenAttrMap(a) else List[Accessor]() 
          val annotationIndex = aggregateOrder.indexOf(a)
          val prevAnnotation = if(annotationIndex < 1) None else Some(aggregateOrder(annotationIndex-1))
          val lastAnnotation = annotationIndex == aggregateOrder.length
          Some(new Annotation(aggregates(a),aggregateExpression,passedAnnotations,prevAnnotation,lastAnnotation))
        } else None

        cgenGHDNode.addAttribute(new CodeGenAttr(a,accessors,annotation,annotated,selection,selectionBelow,prevMaterialized,materialize,lastMaterialized))
        noWork &&= ((accessors.length == 1) && (attrOrder.indexOf(a) == accessors.head.level) && !annotation.isDefined && !selection.isDefined)  //all just existing relations?
      })

      //check for an equivalent bag...stop at the first one
      val equivBags = seenNodes.toList.filter(sn => {sn.equals(cgenGHDNode)})
      val equivTrie = 
        if(noWork) Some(cgenGHDNode.attributeNodes.head.accessors.head.trieName) //take the equivalent relation name
        else if(equivBags.length != 0) Some(equivBags.head.result.name) //take the equivalent result bag name
        else None //no dice we have to compute it

      seenNodes += cgenGHDNode
      seenGHDNodes += ((ghd,parentAttrs))

      //generate the NPRR code
      cgenGHDNode.generateNPRR(s,equivTrie)

      if(topDownUnecessary) 
        CodeGen.emitRewriteOutputTrie(s,lhsName,"bag_" + name,scalarResult)
    }))
    
    //////top down code
    //at the root generate the top down pass if we need it
    val topDown = (!topDownUnecessary)
    if(topDown){
      //cgenGHDNode.generateTopDown() TO DO ... migrate code into here.

      //NEEDS TO BE REFACTORED HARD CORE
      //create top down object
      //ensure that the first accessor is the one with valid data (thus the reverse on seen Nodes) and
      //the other accessors are simply tries that we are indexing into
      val topDownAttrMap = seenGHDNodes.toList.reverse.flatMap(sn => {
        val (cn,parents) = sn
        val currAttrs = cn.attrSet.toList.sortBy(attributeOrdering.indexOf(_))
        val childName = cn.getName(currAttrs)
        val sharedAttrs = parents //appear in the output or are shared
        val materialized = lhs.attrs.union(sharedAttrs)
        val curAttrsIn = currAttrs.filter(materialized.contains(_))
        curAttrsIn.map(a => {
          (a,new Accessor("bag_"+childName,curAttrsIn.indexOf(a),curAttrsIn)) 
        }).filter(tup => tup._1 != "")
      }).groupBy(t => t._1).map(t => (t._1 -> (t._2.map(_._2).distinct)) )
      val td = topDownAttrMap.map(_._1).toList.sortBy(attributeOrdering.indexOf(_)).map(lhsa => {
        new CodeGenTopDown(lhsa,topDownAttrMap(lhsa))
      })

      val aggregateTopDown = aggregates.toList.map(_._1).sortBy(attributeOrdering.indexOf(_))
      val bagLHS = new QueryRelation(lhsName,lhs.attrs)
      val annotatedAttrIn = 
        if(lhs.attrs.length != 0 && aggregateTopDown.size != 0) 
          Some(lhs.attrs.sortBy(attributeOrdering.indexOf(_)).last)
        else None
      val dummy_bag = new CodeGenGHD(bagLHS,List[CodeGenNPRRAttr](),aggregateExpression,scalarResult,aggregateTopDown,annotatedAttrIn,attrToEncodingMap)
      CodeGen.emitNPRR(s,lhsName,dummy_bag,None,Some(td))

      //end refactor portion
    } 
    ///end top down

    CodeGen.emitStopQueryTimer(s)

    val lhsEncodings = lhsOrder.map(i => attrToEncodingMap(lhs.attrs(i))._1).toList
    val lhsTypes = lhsOrder.map(i => attrToEncodingMap(lhs.attrs(i))._2).toList
    val annotations = (lhs.attrs.filter(!attributeOrdering.contains(_)).mkString("_"))
    
    if(!scalarResult){
      //below here should probably be refactored. this saves the environment and writes the trie to disk
      Environment.addBrandNewRelation(lhs.name,new Relation(lhsName,lhsTypes,lhsEncodings))
      Utils.writeEnvironmentToJSON()

      s"""mkdir -p ${Environment.dbPath}/relations/${lhs.name} ${Environment.dbPath}/relations/${lhs.name}/${lhsName}""" !
      
      CodeGen.emitWriteBinaryTrie(s,lhs.name,lhsName)
    } else{
      s.println(s"""std::cout << "Query Result: " << Trie_${lhsName}->annotation << std::endl;""") 
    }
  }
}
