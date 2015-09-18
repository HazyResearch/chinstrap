package DunceCap

import java.util
import scala.collection.immutable.TreeSet
import scala.collection.mutable.ListBuffer

import argonaut.Argonaut._
import argonaut.Json
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType

class CodeGenTopDown(val attr:String,val accessors:List[Accessor])

class CodeGenNPRRAttr(val attr:String, 
  val agg:Option[String], 
  val accessors:List[Accessor], 
  val selection:Option[SelectionCondition], 
  val materialize:Boolean, 
  val first:Boolean, 
  val last:Boolean, 
  val prev:Option[String],
  val materializeViaSelectionsBelow:Boolean,
  val checkSelectionNotMaterialize:Boolean)

class CodeGenGHD(val lhs:QueryRelation, val attrs:List[CodeGenNPRRAttr], 
  val expression:String, val scalarResult:Boolean, 
  val aggregates:List[String], val annotatedAttr:Option[String],
  val attrToEncoding:Map[String,(String,String)])


/*
  Rules for attr order:
    (1) Aggregates must come after materialized attrs.
    (2) Materialized attributes must come before those projected away
    except:
      +Equality selections can come before everything else.
    (3) Selected attributes that are projected away must occur last or first (no exceptions)

  The central object for code generation. Each GHDNode should construct
  a CodeGenGHD node during the bottom-up pass. Then the object calls its
  NPRR code generator. The object contains a nested type which is CodeGenAttr.
  This specifies information needed for each attribute in the bag (selections,
  projections etc.)
*/

//Object that allows us to access and name tries easily
class Accessor(
  val trieName:String, 
  val level:Int, 
  val attrs:List[String], 
  val annotation:Boolean = false 
){
  def printData() : Unit = {
    println("\t\ttrieName: " + trieName + " level: " + level + " attrs: " + attrs + " annotation: " + annotation)
  }
  def getName() : String = {
    if(level == 0)
      "TrieBlock_" + trieName + "_" + level
    else {
      val name = "TrieBlock_" + trieName + "_" + level + "_" + (0 to level).map(i => attrs(i)).mkString("_")
      return name
    }
  }
  def getPrevName() : String = {
    if(level-1 == 0)
      "TrieBlock_" + trieName + "_0"
    else{
      val name = "TrieBlock_" + trieName + "_" + (level-1) + "_" + (0 to (level-1)).map(i => attrs(i)).mkString("_")
      return name
    }
  }
  def getPrevAttr() : String = {
    return attrs(level-1)
  }
  override def equals(that: Any): Boolean =
    that match {
      case that: Accessor => that.trieName.equals(trieName) && that.level.equals(level) && that.annotation.equals(annotation)
      case _ => false
    }
}

class Annotation(val operation:String,
  val init:String, 
  val expression:String, 
  val prev:Option[String],
  val next:Option[String]) {

  override def equals(that: Any): Boolean =
    that match {
    case that: Annotation => that.init.equals(init) && that.expression.equals(expression) && 
      that.next.isDefined.equals(next.isDefined) && that.prev.isDefined.equals(prev.isDefined)
    case _ => false
  }

  def printData() = {
    println("\t\toperation: " + operation)
    println("\t\tinit: " + init)
    println("\t\texpression: " + expression)
    println("\t\tprev: " + prev)
    println("\t\tnext: " + next)
  }
}

class SelectionCondition(val condition:String,val value:String){
  def printData() : Unit = {
    println("\t\tcondition: " + condition + " value: " + value)
  }
  override def equals(that: Any): Boolean =
  that match {
    case that: SelectionCondition => that.condition.equals(condition) && that.value.equals(value)
    case _ => false
  }
}

class CodeGenAttr (
  val attr:String, //(current attribute name)
  val accessors:List[Accessor], //tries we access into (contains those passed from children as well)
  val annotation:Option[Annotation], //aggregations
  val annotated:Option[String],
  val selection:Option[SelectionCondition], //selections
  val selectionBelow:Boolean, //selections below this attribute
  val nextMaterialized:Option[String],
  val prevMaterialized:Option[String],
  val materialize:Boolean,  //projections
  val lastMaterialized:Boolean//tells you if you are annotated attr
) {

  def printData() = {
    println("\tattr: " + attr)
    println("\tannotated: " + annotated)
    println("\tselectionBelow: " + selectionBelow)
    println("\taccessors: ")
    accessors.foreach(_.printData())
    if(annotation.isDefined){
      println("\tannotation: ")
      annotation.get.printData()
    }
    else
      println("\tannotation: " + annotation)
    if(selection.isDefined)
      println("\tselection: " + selection.get.printData())
    else 
      println("\tselection: " + selection)
    println("\tmaterialize: " + materialize)
    println("\tlastMaterialize: " + lastMaterialized)
    println("\tprevMaterialized: " + prevMaterialized)
    println("\tnextMaterialized: " + nextMaterialized)

  }
  override def equals(o: Any) = o match {
    case that: CodeGenAttr => {
      val annotationEquiv = 
      if(annotation.isDefined && that.annotation.isDefined) 
        annotation == that.annotation
      else 
        annotation.isDefined == that.annotation.isDefined
      annotationEquiv && 
      selection == that.selection &&
      accessors.length == that.accessors.length &&
      accessors.map(acc1 =>{
        accessors.map(acc2 => {
          acc1 == acc2
        }).reduce((a,b) => a || b) //each accessor has an equivalent one
      }).reduce( (a,b) => a && b) //they all must match an equivalent one
    }
    case _ => false
  }
}

class CodeGenGHDNode(
  val joinType:String,
  val recursive:Boolean,
  val transitiveClosure:Boolean,
  val result:QueryRelation,
  val scalarResult:Boolean, 
  val attrToEncoding:Map[String,(String,String)]) {
  var attributeNodes = new ListBuffer[CodeGenAttr]()

  def printData() = {
    println("join type: " + joinType)
    println("recursive: " + recursive)
    println("transitiveClosure: " + transitiveClosure)
    println("result: " + result.name + " " + result.attrs)
    println("scalarResult: " + scalarResult)
    println("attrToEncoding: " + attrToEncoding)
    attributeNodes.foreach(_.printData())
  }

  //add an attribute to our linked list of attrs to process
  def addAttribute(in:CodeGenAttr) = {
    attributeNodes += in
  }

  //emit the NPRR code (part of the bottom up pass)
  def generateNPRR(s:CodeStringBuilder,noWork:Option[String]) : Unit = {
    if(Environment.debug)
      this.printData()
    
    //get the bag started
    CodeGen.emitAllocateResultTrie(s,noWork,result)

    if(noWork.isDefined)
      return

    //Some intialization before the actual algorithm
    attributeNodes.foreach(an =>{
      //Each annotation has a buffer to conserve memory usage
      if(an.annotation.isDefined)
        CodeGen.emitAllocateAggregateBuffer(s,an.attr)
      
      //emit the values for the selection (must lookup because we dictionary encode)
      an.selection match{
        case Some(selection) =>{
          val (encoding,atype) = attrToEncoding(an.attr)
          CodeGen.emitSelectionValue(s,an.attr,encoding,selection.value)
        } case None =>
      }
    })

    var seenAccessors = Set[String]() //don't duplicate accessor definitions
    var seenSelections = Set[String]() //don't duplicate accessor definitions

    //go forward through the attributes
    var parallelSelectionMaterialized = false
    (0 until attributeNodes.length).foreach(i => {
      val curr = attributeNodes(i)
      val tid = if( (!curr.prevMaterialized.isDefined && !scalarResult) || (i==0)) "0" else "tid"
      val first = i == 0
      val last = i == attributeNodes.length-1

      parallelSelectionMaterialized ||= (first && curr.selectionBelow && curr.lastMaterialized)

      curr.accessors.filter(acc => !seenAccessors.contains(acc.getName())).foreach(accessor => {
        if( accessor.level != 0  && seenSelections.contains(accessor.getPrevAttr()))
          CodeGen.emitSelectedTrieBlock(s,curr.attr,accessor)
        else
          CodeGen.emitTrieBlock(s,curr.attr,accessor)
      })
      
      if(curr.selection.isDefined)
        seenSelections += curr.attr
      seenAccessors ++= curr.accessors.map(_.getName())

      CodeGen.emitMaxSetAlloc(s,curr.attr,curr.accessors)
      
      //Build the new Set 
      val setName = if(curr.selectionBelow && curr.lastMaterialized) s"""${curr.attr}_filtered""" else curr.attr
      val bufferName = if(curr.annotation.isDefined && curr.annotation.get.next.isDefined) s"""${curr.attr}_buffer""" else if(curr.lastMaterialized && curr.selectionBelow) "tmp_buffer" else "output_buffer"
      val otherBufferName = if(curr.selectionBelow && curr.lastMaterialized) "output_buffer" else "tmp_buffer" //trick to save mem when
      
      ///TODO: Refactor into code gen object
      if(curr.accessors.length == 1){
        (curr.selection,curr.materialize,last,curr.prevMaterialized) match {
          case (Some(selection),true,_,_) => CodeGen.emitMaterializedSelection(s,curr.attr,curr.selectionBelow,tid,selection,s"""${curr.accessors.head.getName()}->set""")
          case (Some(selection),false,true,Some(prevMaterialized)) => CodeGen.emitFinalCheckConditions(s,selection,curr.attr,s"""${curr.accessors.head.getName()}->set""",parallelSelectionMaterialized,prevMaterialized)
          case (Some(selection),false,false,_) => CodeGen.emitCheckConditions(s,selection,curr.attr,s"""${curr.accessors.head.getName()}->set""")
          case (_,_,_,_) => s.println(s"""Set<${Environment.layout}> ${setName} = ${curr.accessors.head.getName()}->set;""")
        }
      } else if(curr.accessors.length == 2){
        s.println(s"""Set<${Environment.layout}> ${setName}(${bufferName}->get_next(${tid},alloc_size_${curr.attr}));""")
        CodeGen.emitIntersection(s,setName,curr.accessors.head.getName()+"->set",curr.accessors.last.getName()+"->set",curr.selection,curr.attr)
      } else if(curr.accessors.length > 2){
        s.println(s"""Set<${Environment.layout}> ${setName}(${bufferName}->get_next(${tid},alloc_size_${curr.attr}));""")
        s.println(s"""Set<${Environment.layout}> ${curr.attr}_tmp(${otherBufferName}->get_next(${tid},alloc_size_${curr.attr})); //initialize the memory""")
        var tmp = (curr.accessors.length % 2) == 1
        var name = if(tmp) s"""${curr.attr}_tmp""" else setName
        CodeGen.emitIntersection(s,name,curr.accessors(0).getName()+"->set",curr.accessors(1).getName()+"->set",curr.selection,curr.attr)
        (2 until curr.accessors.length).foreach(i => {
          tmp = !tmp
          name = if(tmp) s"""${curr.attr}_tmp""" else setName
          val opName = if(!tmp) s"""${curr.attr}_tmp""" else setName
          CodeGen.emitIntersection(s,name,opName,curr.accessors(i).getName()+"->set",None,curr.attr)
        })
        s.println(s"""${otherBufferName}->roll_back(${tid},alloc_size_${curr.attr});""")
      }

      //save some memory
      if(curr.accessors.length != 1 && !curr.annotation.isDefined){
        CodeGen.emitRollBack(s,curr.attr,(curr.selectionBelow && curr.lastMaterialized),tid)
      }
      if(curr.selectionBelow && curr.lastMaterialized) CodeGen.emitSetupFilteredSet(s,tid=="0",curr.attr,tid) //must occur after roll back
      else if(curr.materialize) CodeGen.emitNewTrieBlock(s,curr.attr,tid,curr.lastMaterialized,curr.annotated.isDefined)
      
      if(curr.annotation.isDefined) CodeGen.emitAnnotationInitialization(s,curr.attr,curr.annotation.get,tid,scalarResult)


      if( (curr.lastMaterialized && curr.annotated.isDefined) || (!curr.lastMaterialized && curr.materialize) ){ //annotated attribute or not the last materialized
        CodeGen.emitForEachIndex(s,curr.attr,tid=="0")
      }
      else if(curr.lastMaterialized && curr.selectionBelow && !curr.nextMaterialized.isDefined && !curr.annotated.isDefined){ //materialized with a selection below & no annotations
        CodeGen.emitForEach(s,curr.attr + "_filtered",curr.attr,tid=="0")
      } 

      if(curr.annotation.isDefined && (curr.accessors.map(_.annotation).reduce((a,b) => a || b) || curr.annotation.get.next.isDefined) ) { //just an aggregate
        CodeGen.emitForEach(s,curr.attr,curr.attr,tid=="0")
      } 
      //annotation computation on the last level 
      if(curr.annotation.isDefined && !curr.annotation.get.next.isDefined) 
        CodeGen.emitAnnotationComputation(s,curr.attr,curr.annotation.get,curr.accessors)
    })

    //now do a backward pass over the attributes
    (0 until attributeNodes.length).foreach(j => {
      val i = attributeNodes.length-1-j //go in reverse now
      val first = i == 0
      val last = i == attributeNodes.length-1

      val curr = attributeNodes(i)
      val tid = if( (!curr.prevMaterialized.isDefined && !scalarResult) || (i==0)) "0" else "tid"

      if(curr.selection.isDefined && !curr.materialize && !last){
        s.println("}")
      }

      if(curr.materialize){
        CodeGen.emitSetTrieBlock(s,curr.attr,curr.lastMaterialized,curr.nextMaterialized,result.name,curr.annotated)
      }

      val annotationCondition = curr.annotation.isDefined && (curr.accessors.map(_.annotation).reduce((a,b) => a || b) || curr.annotation.get.next.isDefined)
      val forCondition = ((curr.lastMaterialized && curr.annotated.isDefined) || (!curr.lastMaterialized && curr.materialize)) ||
        (curr.lastMaterialized && curr.selectionBelow && !curr.annotated.isDefined) ||
        annotationCondition

      //close out for loop
      if(forCondition)
        s.println("});")

      if(curr.selectionBelow && curr.lastMaterialized) CodeGen.emitBuildNewSet(s,tid=="0",curr.lastMaterialized,curr.attr,tid)

      if(curr.annotation.isDefined && curr.accessors.length > 1){ //annotations can free memory
        if(curr.annotation.get.next.isDefined)
          CodeGen.emitRollBackALL(s,curr.attr,curr.attr+"_buffer",tid)
        else
          CodeGen.emitRollBackALL(s,curr.attr,"output_buffer",tid)
      }

      if(curr.annotation.isDefined && !(i == 1 && scalarResult) && curr.annotation.get.expression != "")
        CodeGen.emitAnnotationScalarExpression(s,curr.attr,curr.annotation.get)

      if(curr.annotation.isDefined && curr.annotation.get.prev.isDefined){ //update except when we have a scalar result
        val prev = attributeNodes(i-1) //a little weird but necessary for parallelization, lift multiplication outside of inner loop
        if(!(i == 1 && scalarResult))
          CodeGen.emitUpdateAnnotation(s,curr.attr,curr.annotation.get,prev.attr,prev.accessors)
        else 
          CodeGen.emitUpdateAnnotationReducer(s,curr.attr,tid,curr.annotation.get,prev.attr,prev.accessors)
      } else if(curr.annotation.isDefined && !curr.annotation.get.prev.isDefined && scalarResult){
        CodeGen.emitScalarAnnotation(s,result.name,curr.annotation.get.expression)
      }

      if(curr.materialize && !curr.prevMaterialized.isDefined)
        CodeGen.emitSetTrie(s,result.name,curr.attr)

    })

    //Each annotation has a buffer to conserve memory usage
    attributeNodes.foreach(an =>{
      if(an.annotation.isDefined)
        CodeGen.emitFreeAggregateBuffer(s,an.attr)
    })
    CodeGen.emitFinishNPRR(s,result)
  }

  //emit the top down pass
  def generateTopDown() = {
    println("Emitting Top Down")
  }

  override def equals(o: Any) = o match {
    case that: CodeGenGHDNode => {
      scalarResult == that.scalarResult &&
      result.attrs.length == that.result.attrs.length &&
      attributeNodes == that.attributeNodes
    }
    case _ => false
  }
}

class GHDNode(val rels: List[QueryRelation]) {
  val attrSet = rels.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel : QueryRelation) => accum | TreeSet[String](rel.attrs : _*))
  var children: List[GHDNode] = List()
  var bagWidth: Int = 0
  var bagFractionalWidth: Double = 0
  var attributeReordering : Option[Map[String, Int]] = None
  final val ATTRIBUTE_NUMBERING_START = 0
  var num_bags:Int = 0
  var depth:Int = 0
  var codeGen:CodeGenGHD = null

  override def equals(o: Any) = o match {
    case that: GHDNode => that.rels.equals(rels) && that.children.equals(children)
    case _ => false
  }

  override def hashCode = 41 * rels.hashCode() + children.hashCode()

  def getName(attribute_ordering:List[String]): String = {
    this.rels.map(r => r.name).distinct.mkString("") + "_" + attribute_ordering.mkString("")
  }

  def getNumBags(): Int = {
    1 + children.foldLeft(0)((accum : Int, child : GHDNode) => accum + child.getNumBags())
  }

  def scoreTree(): Int = {
    bagWidth = attrSet.size
    return children.map((child: GHDNode) => child.scoreTree()).foldLeft(bagWidth)((accum: Int, x: Int) => if (x > accum) x else accum)
  }

  private def getMatrixRow(attr : String, rels : List[QueryRelation]): Array[Double] = {
    val presence = rels.map((rel : QueryRelation) => if (rel.attrs.toSet.contains(attr)) 1.0 else 0)
    return presence.toArray
  }

  private def fractionalScoreNode(): Double = { // TODO: catch UnboundedSolutionException
    val objective = new LinearObjectiveFunction(rels.map((rel : QueryRelation) => 1.0).toArray, 0)
    // constraints:
    val constraintList = new util.ArrayList[LinearConstraint]
    attrSet.map((attr : String) => constraintList.add(new LinearConstraint(getMatrixRow(attr, rels), Relationship.GEQ,  1.0)))
    val constraints = new LinearConstraintSet(constraintList)
    val solver = new SimplexSolver
    val solution = solver.optimize(objective, constraints, GoalType.MINIMIZE, new NonNegativeConstraint(true))
    return solution.getValue
  }

  def fractionalScoreTree() : Double = {
    bagFractionalWidth = fractionalScoreNode()
    return children.map((child: GHDNode) => child.fractionalScoreTree())
      .foldLeft(bagFractionalWidth)((accum: Double, x: Double) => if (x > accum) x else accum)
  }

  private def toJson(reorderFn: (String => String)): Json = {
    val relationsJson = jArray(rels.map((rel : QueryRelation) => Json("attrs" -> jArray(rel.attrs.map((str: String) => jString(reorderFn(str)))))))
    if (!children.isEmpty) {
      return Json("relations" -> relationsJson, "children" -> jArray(children.map((child: GHDNode) => child.toJson(reorderFn))))
    } else {
      return Json("relations" -> relationsJson)
    }
  }

  def toJson(): Json = {
    return toJson(translateAttribute)
  }

  private def getAttributeReordering(reordering : Map[String, Int], newAttrName : Int): (Map[String, Int], Int) = {
    val newAttrs : Set[String] = attrSet.filter((attr : String) => !reordering.contains(attr))
    val (newReordering, updatedNewAttrName) = newAttrs.foldLeft(
      (reordering, newAttrName))((accum: (Map[String, Int], Int), origAttrName: String) => (accum._1 + (origAttrName -> accum._2), accum._2 +1))
    return children.foldLeft(
      (newReordering, updatedNewAttrName))((accum: (Map[String, Int], Int), child : GHDNode) => child.getAttributeReordering(
      accum._1, accum._2))
  }

  def reorderAttributes() = {
    attributeReordering = Some(getAttributeReordering(Map(), ATTRIBUTE_NUMBERING_START)._1)
  }

  private def translateAttribute(attr : String) = {
    attributeReordering.fold(attr)((reordering: Map[String, Int]) => "attr_" + attr)
  }
}