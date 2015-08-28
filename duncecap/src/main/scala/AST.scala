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
    val relations = join.map(qr => new QueryRelation(qr.name,qr.attrs.map{_._1}))
    val selections = join.flatMap(qr => qr.attrs.filter(atup => atup._2 != "").map(atup => (atup._1,new SelectionCondition(atup._2,atup._3)) ) ).toMap

    val myghd = GHDSolver.getGHD(relations) //get minimum GHD's

    //find attr ordering
    val attributeOrdering = GHDSolver.getAttributeOrdering(myghd,lhs.attrs)
    
    if(Environment.debug)
      println("GLOBAL ATTR ORDER: " + attributeOrdering)
    
    //map each attribute to an encoding and type
    val attrToEncodingMap = relations.flatMap(qr => {
      val rName = qr.name + "_" + (0 until qr.attrs.length).mkString("_")
      (0 until qr.attrs.length).map(i => {
        (qr.attrs(i) -> (Environment.relations(qr.name)(rName).encodings(i),Environment.relations(qr.name)(rName).types(i)))
      }).toList.distinct
    }).toMap
    println("ATTR TO ENCODING MAP: " + attrToEncodingMap)

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
    val topDownUnecessary = myghd .num_bags == 1 ||  
      (lhs.attrs.intersect(myghd.attrSet.toList).length == lhs.attrs.length)
    println("topDownUnecessary: " + topDownUnecessary)

    val lhsOrder = (0 until lhs.attrs.length).filter(i => attributeOrdering.contains(lhs.attrs(i))).sortBy(i => attributeOrdering.indexOf(lhs.attrs(i))).toList
    val lhsName = lhs.name+"_"+lhsOrder.mkString("_")
    val scalarResult = lhsOrder.length == 0

    val aggregateExpression = if(aggregateExpressions.isEmpty) "" else aggregateExpressions.head._2

    //run algorithm
    val seenNPRRBags = mutable.ListBuffer[CodeGenGHD]()
    val seenNodes = mutable.ListBuffer[(GHDNode,List[String])]()

    GHDSolver.bottomUp(myghd, ((ghd:GHDNode,root:Boolean,parent:GHDNode) => {
      val attrOrder = ghd.attrSet.toList.sortBy(attributeOrdering.indexOf(_))
      val lhsAttrs = lhs.attrs.filter(attrOrder.contains(_)).sortBy(attrOrder.indexOf(_))
      val childAttrs = ghd.children.flatMap(child => {child.attrSet}).toList.distinct
      val parentAttrs = if(!root) parent.attrSet.toList else List()
      val sharedAttrs = attrOrder.intersect(childAttrs.union(parentAttrs).distinct)
      val materializedAttrs = lhsAttrs.union(sharedAttrs).distinct.sortBy(attributeOrdering.indexOf(_))
      seenNodes += ((ghd,parentAttrs))

      val name = ghd.getName(attrOrder)
      val bagLHS = new QueryRelation("bag_" + name,lhsAttrs)

      //for the relation to be in the bag all of its attributes must appear in the GHD
      val bagRelations = reorderedRelations.filter(rr => {
        rr._2.attrs.intersect(attrOrder).length == rr._2.attrs.length
      })

      //figure out if there are selections below the last materialized attribute
      val lastMaterializedAttrIndex = if(materializedAttrs.length != 0) attrOrder.indexOf(materializedAttrs.last) else attrOrder.length
      val selectionAfterLast = if(selections.size != 0) 
        selections.map(op => {attrOrder.indexOf(op._1) > lastMaterializedAttrIndex}).reduce((a,b) => a || b)
        else 
          false

      //any attr that comes before gets passed
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
      println("AGG ORDER: " + aggregateOrder)
      //annotate the last materialized attribute with a aggregate below it
      println("MATERIALIZE ATTRS: " + materializedAttrs)
      val annotatedAttr = 
        if(materializedAttrs.length != 0 && aggregates.size != 0) 
          Some(materializedAttrs.last)
        else None

      val cgScalarResult = (materializedAttrs.length == 0) && (aggregates.size != 0)
      val codeGenAttrs = attrOrder.map(a => {
        //create aggregate
        val materialize = materializedAttrs.contains(a)
        val first = a == attrOrder.head 
        val last = a == attrOrder.last
        val prev = if(!first) Some(attrOrder(attrOrder.indexOf(a)-1)) else None
        val selection = if(selections.contains(a)) Some(selections(a)) else None
        val materializeViaSelectionsBelow = if(selectionAfterLast && a == materializedAttrs.last) true else false
        val checkSelectionNotMaterialize = if(selectionAfterLast && attrOrder.indexOf(a) > attrOrder.indexOf(materializedAttrs.last)) true else false
        val bagAccessors = bagRelations.
          filter(rr => rr._2.attrs.contains(a)).
          map(rr => new Accessor(rr._2.name,rr._2.attrs.indexOf(a),rr._2.attrs) ).
          groupBy(a => a.getName()).map(_._2.head).toList
        //those shared in the children
        val sharedAccessors = if(childrenAttrMap.contains(a)) childrenAttrMap(a) else List[Accessor]()
        val accessors = bagAccessors ++ sharedAccessors
        val aggregate = if(aggregates.contains(a) && !materialize) Some(aggregates(a)) else None

        new CodeGenNPRRAttr(a,aggregate,accessors,selection,materialize,
          first,last,prev,materializeViaSelectionsBelow,checkSelectionNotMaterialize)
      })

      //first check if the bag just produces an existing relation
      val noWork1 = codeGenAttrs.map(cga => {
        ( (cga.accessors.length == 1) 
          && (attrOrder.indexOf(cga.attr) == cga.accessors.head.level)
          && !cga.agg.isDefined && !cga.selection.isDefined )
      }).reduce( (a,b) => a && b)
      
      //next check if the bag is equivalent to one we already processed
      val noWork2 = seenNPRRBags.toList.map(cg =>{
        val equivBag = ( (cg.lhs.attrs.length == lhsAttrs.length) && 
          (cg.attrs.length == codeGenAttrs.length) &&
        ((0 until cg.attrs.length).map(i => {
          //check if the selections are the same
          val optionSel1 = cg.attrs(i).selection
          val optionSel2 = codeGenAttrs(i).selection 
          val selectionsSame = (optionSel1,optionSel2) match {
            case (Some(sel1),Some(sel2)) => {
              println("HERE HERE HERE")
              (sel1.condition == sel2.condition) && (sel1.value == sel2.value)
            }
            case (None,None) => true
            case (_,_) => false
          }
          val acc1 = cg.attrs(i).accessors.toSet
          val acc2 = codeGenAttrs(i).accessors.toSet
          val accessorsSame = (acc1 == acc2)
          println("SELECTIONS SAME: " + selectionsSame)
          (accessorsSame && selectionsSame)
        }).reduce( (a,b) => a && b) ) )
        if(equivBag) Some(cg.lhs.name)
        else None
      })

      println("NO WORK 2: " + noWork2)

      val noWork = 
        if(noWork1) Some(codeGenAttrs.head.accessors.head.trieName) 
        else if(noWork2.length > 0) noWork2.head
        else None

      val myghdbag = new CodeGenGHD(bagLHS,codeGenAttrs,aggregateExpression,cgScalarResult,aggregateOrder,annotatedAttr,attrToEncodingMap)
      seenNPRRBags += myghdbag

      val topDown = (Environment.yanna == true && root && !topDownUnecessary)
      if(!topDown){
        CodeGen.emitNPRR(s,name,myghdbag,noWork,None)
        if(root)
          CodeGen.emitRewriteOutputTrie(s,lhsName,"bag_" + name,scalarResult)
      } else {
        println("HERE HERE HERE HERE")
        //create top down object
        //ensure that the first accessor is the one with valid data (thus the reverse on seen Nodes) and
        //the other accessors are simply tries that we are indexing into
        val topDownAttrMap = seenNodes.toList.reverse.flatMap(sn => {
          val (cn,parents) = sn
          val currAttrs = cn.attrSet.toList.sortBy(attributeOrdering.indexOf(_))
          val childName = cn.getName(currAttrs)
          val sharedAttrs = (cn.children.flatMap(p => p.attrSet.toList)).union(parents) //appear in the output or are shared
          println("bag: " + childName)
          println("currAttrs: " + currAttrs)
          println("sharedAttrs: " + sharedAttrs)
          val materialized = lhs.attrs.union(sharedAttrs)
          val curAttrsIn = currAttrs.filter(materialized.contains(_))
          curAttrsIn.map(a => {
            (a,new Accessor("bag_"+childName,curAttrsIn.indexOf(a),curAttrsIn)) 
          }).filter(tup => tup._1 != "")
        }).groupBy(t => t._1).map(t => (t._1 -> (t._2.map(_._2).distinct)) )
        val td = topDownAttrMap.map(_._1).toList.sortBy(attributeOrdering.indexOf(_)).map(lhsa => {
          new CodeGenTopDown(lhsa,topDownAttrMap(lhsa))
        })

        if(Environment.pipeline){
          CodeGen.emitNPRR(s,name,myghdbag,None,Some(td.filter(!attrOrder.contains(_)))) //filter out the attrs in the bag for pipelined top down
          CodeGen.emitRewriteOutputTrie(s,lhsName,"bag_" + name,scalarResult)
        } else {
          CodeGen.emitNPRR(s,name,myghdbag,noWork,None)

          val aggregateTopDown = aggregates.toList.map(_._1).sortBy(attributeOrdering.indexOf(_))
          val bagLHS = new QueryRelation(lhsName,lhs.attrs)
          val annotatedAttrIn = 
            if(lhsAttrs.length != 0 && aggregateTopDown.size != 0) 
              Some(lhsAttrs.last)
            else None
          val dummy_bag = new CodeGenGHD(bagLHS,List[CodeGenNPRRAttr](),aggregateExpression,scalarResult,aggregateTopDown,annotatedAttrIn,attrToEncodingMap)
          CodeGen.emitNPRR(s,lhsName,dummy_bag,None,Some(td))
        }
      }
    }))

    CodeGen.emitStopQueryTimer(s)

    val lhsEncodings = lhsOrder.map(i => attrToEncodingMap(lhs.attrs(i))._1).toList
    val lhsTypes = lhsOrder.map(i => attrToEncodingMap(lhs.attrs(i))._2).toList
    val annotations = (lhs.attrs.filter(!attributeOrdering.contains(_)).mkString("_"))

    if(!scalarResult){
      //below here should probably be refactored. this saves the environment and writes the trie to disk
      println("LHS Encodings: " + lhsEncodings)
      Environment.addBrandNewRelation(lhs.name,new Relation(lhsName,lhsTypes,lhsEncodings))
      Utils.writeEnvironmentToJSON()

      s"""mkdir -p ${Environment.dbPath}/relations/${lhs.name} ${Environment.dbPath}/relations/${lhs.name}/${lhsName}""" !
      
      CodeGen.emitWriteBinaryTrie(s,lhs.name,lhsName)
    } else{
      s.println(s"""std::cout << "Query Result: " << Trie_${lhsName}->annotation << std::endl;""") 
    }
  }
}
