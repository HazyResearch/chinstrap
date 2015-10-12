package DunceCap

import java.util

import DunceCap.attr.Attr
import argonaut.Argonaut._
import argonaut.Json
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType

import scala.collection.immutable.TreeSet


object GHD {
  def getNumericalOrdering(attributeOrdering:List[Attr], rel:QueryRelation): List[Int] = {
    attributeOrdering.map(a => rel.attrNames.indexOf(a)).filter(pos => {
      pos != -1
    })
  }
}

class GHD(val root:GHDNode,
          val queryRelations: List[QueryRelation],
          val joinAggregates:Map[String,ParsedAggregate],
          val outputRelation: QueryRelation) {
  val attributeOrdering: List[Attr] = GHDSolver.getAttributeOrdering(root, outputRelation.attrNames)
  var depth: Int = -1
  var numBags: Int = -1

  def toJson(): Json = {
    Json(
      "query_type" -> jString("join"),
      "relations" -> getRelationsSummary(),
      "output" -> getOutputInfo(),
      "ghd" -> jArray(getJsonFromPreOrderTraversal(root))
    )
  }

  /**
   * Summary of all the relations in the GHD
   * @return Json for the relation summary
   */
  private def getRelationsSummary(): Json = {
    jArray(getRelationSummaryFromPreOrderTraversal(root).distinct)
  }

  private def getRelationSummaryFromPreOrderTraversal(node:GHDNode): List[Json] = {
    node.getJsonRelationInfo(true):::node.children.flatMap(c => {getRelationSummaryFromPreOrderTraversal(c)})
  }

  /**
   *"output":{
		"name":"TriangleCount",
		"ordering":[],
		"annotation":"long"
	  },
   */
  private def getOutputInfo(): Json = {
    Json(
      "name" -> jString(outputRelation.name),
      "ordering" -> jArray(GHD.getNumericalOrdering(attributeOrdering, outputRelation).map(o => jNumber(o))),
      "annotation" -> jString(outputRelation.annotationType)
    )
  }

  private def getJsonFromPreOrderTraversal(node:GHDNode): List[Json] = {
    node.getJsonBagInfo(joinAggregates)::node.children.flatMap(c => getJsonFromPreOrderTraversal(c))
  }

  /**
   * Do a post-processiing pass to fill out some of the other vars in this class
   * You should call this before calling toJson
   */
  def doPostProcessingPass() = {
    root.computeDepth
    depth = root.depth
    numBags = root.getNumBags()
    root.setBagName(outputRelation.name)
    root.setDescendantNames(1)
    root.setAttributeOrdering(attributeOrdering)
    root.computeProjectedOutAttrsAndOutputRelation(outputRelation.attrNames.toSet, Set())
    root.createAttrToRelsMapping
  }
}


class GHDNode(var rels: List[QueryRelation]) {
  val attrSet = rels.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: QueryRelation) => accum | TreeSet[String](rel.attrNames: _*))
  var subtreeRels = rels
  var bagName: String = null
  var attrToRels:Map[Attr,List[QueryRelation]] = null
  var attributeOrdering: List[Attr] = null
  var children: List[GHDNode] = List()
  var bagFractionalWidth: Double = 0
  var bagWidth: Int = 0
  var depth: Int = 0
  var projectedOutAttrs: Set[Attr] = null
  var outputRelation: QueryRelation = null

  /**
   * This is intended for use by GHDSolver, so we don't distinguish between trees with different vars set
   * in GHD's post-processing pass
   */
  override def equals(o: Any) = o match {
    case that: GHDNode => that.rels.equals(rels) && that.children.equals(children)
    case _ => false
  }

  override def hashCode = 41 * rels.hashCode() + children.hashCode()

  def setBagName(name:String): Unit = {
    bagName = name
  }

  def getJsonBagInfo(joinAggregates:Map[String,ParsedAggregate]): Json = {
    val jsonRelInfo = getJsonRelationInfo()
    Json(
      "name" -> jString(bagName),
      "attributes" -> jArray(outputRelation.attrNames.map(attrName => {jString(attrName)})),
      "annotation" -> jString(outputRelation.annotationType),
      "relations" -> jArray(jsonRelInfo),
      "nprr" -> jArray(getJsonNPRRInfo(joinAggregates))
    )
  }

  def setDescendantNames(depth:Int): Unit = {
    children.zipWithIndex.map(childAndIndex => {
      childAndIndex._1.setBagName(s"bag_${depth}_${childAndIndex._2}")
    })
    children.map(child => {child.setDescendantNames(depth+1)})
  }

  private def getJsonNPRRInfo(joinAggregates:Map[String,ParsedAggregate]) : List[Json] = {
    val attrsWithAccessorJson = getOrderedAttrsWithAccessorJson()
    val prevAndNextAttrMaterialized = getPrevAndNextAttrMaterialized(
      attrsWithAccessorJson,
      ((attr:Attr) => outputRelation.attrNames.contains(attr)))
    val prevAndNextAttrAggregated = getPrevAndNextAttrMaterialized(
      attrsWithAccessorJson,
      ((attr:Attr) => joinAggregates.get(attr).isDefined))

    attrsWithAccessorJson.zip(prevAndNextAttrMaterialized.zip(prevAndNextAttrAggregated)).flatMap(attrAndPrevNextInfo => {
      val (attr, prevNextInfo) = attrAndPrevNextInfo
      val (materializedInfo, aggregatedInfo) = prevNextInfo
      val accessorJson = getAccessorJson(attr)
      if (accessorJson.isEmpty) {
        None // this should not happen
      } else {
        Some(Json(
          "name" -> jString(attr),
          "accessors" -> jArray(accessorJson),
          "materialize" -> jBool(outputRelation.attrNames.contains(attr)),
          "selection" -> jBool(hasSelection(attr)),
          "materialize" -> jBool(outputRelation.attrNames.contains(attr)),
          "annotation" -> getNextAnnotatedForLastMaterialized(attr, joinAggregates),
          "aggregation" -> getAggregationJson(joinAggregates, attr, aggregatedInfo),
          "prevMaterialized" -> jString(materializedInfo._1),
          "nextMaterialized" -> jString(materializedInfo._2)
         ))
      }
    })
  }

  private def getPrevAndNextAttrMaterialized(attrsWithAccessorJson: List[Attr], filterFn:(Attr => Boolean)): List[(Attr, Attr)] = {
    val prevAttrsMaterialized = attrsWithAccessorJson.foldLeft((List[String](), "None"))((acc, attr) => {
      val prevAttrMaterialized = (
        if (filterFn(attr)) {
          attr
        } else {
          acc._2
        })
      (acc._2::acc._1, prevAttrMaterialized)
    })._1.reverse
    val nextAttrsMaterialized = attrsWithAccessorJson.foldRight((List[String](), "None"))((attr, acc) => {
      val nextAttrMaterialized = (
        if (filterFn(attr)) {
          attr
        } else {
          acc._2
        })
      (acc._2::acc._1, nextAttrMaterialized)
    })._1
    prevAttrsMaterialized.zip(nextAttrsMaterialized)
  }

  private def getNextAnnotatedForLastMaterialized(attr:Attr, joinAggregates:Map[String,ParsedAggregate]): Json = {
    if (!outputRelation.attrNames.isEmpty && outputRelation.attrNames.last == attr) {
      val annotatedAttr = attributeOrdering.dropWhile(a => a != attr).tail.find(a => joinAggregates.contains(a) && attrSet.contains(a))
      jString(annotatedAttr.getOrElse("None"))
    } else {
      jString("None")
    }
  }

  private def getAggregationJson(joinAggregates:Map[String,ParsedAggregate], attr:Attr, prevNextInfo:(Attr, Attr)): Json = {
    val maybeJson = joinAggregates.get(attr).map(parsedAggregate => {
      Json(
        "operation" -> jString(parsedAggregate.op),
        "init" -> jString(parsedAggregate.init),
        "expression" -> jString(parsedAggregate.expression),
        "prev" -> jString(prevNextInfo._1),
        "next" -> jString(prevNextInfo._2)
      )
    })
    maybeJson match {
      case Some(json) => json
      case None => jString("None")
    }
  }

  /**
   * @param attr, an attribute that definitely exists in this bag
   * @return Boolean for whether this attribute has a selection on it or not
   */
  private def hasSelection(attr:Attr): Boolean = {
    !attrToRels.get(attr).getOrElse(List())
      .filter(rel => !rel.attrs.filter(attrInfo => attrInfo._1 == attr).head._2.isEmpty).isEmpty
  }

  private def getOrderedAttrsWithAccessorJson(): List[Attr] = {
    attributeOrdering.flatMap(attr => {
      val accessorJson = getAccessorJson(attr)
      if (accessorJson.isEmpty) {
        None
      } else {
        Some(attr)
      }
    })
  }

  private def getAccessorJson(attr:Attr): List[Json] = {
    attrToRels.get(attr).getOrElse(List()).map(rel => {
      Json(
        "name" -> jString(rel.name),
        "attrs" -> jArray(rel.attrNames.map(attrName => {jString(attrName)})),
        "annotated" -> jBool(rel.attrNames.tail == attr && rel.annotationType != "void*")
      )
    })
  }

  /**
   * Generates the following:
   *
  "relations": [
        {
          "name":"R",
          "ordering":[0,1],
          "attributes":[["a","b"],["b","c"],["a","c"]], # this row is optional
          "annotation":"void*"
        }
      ],
   */
  def getJsonRelationInfo(forTopLevelSummary:Boolean = false): List[Json] = {
    val relsToUse =
      if (forTopLevelSummary) {
        rels }
      else {
        subtreeRels
      }

    val distinctRelationNames = relsToUse.map(r => r.name).distinct
    distinctRelationNames.flatMap(n => {
      val relationsWithName = relsToUse.filter(r => {r.name == n})
      val orderingsAndRels: List[(List[Int], List[QueryRelation])] = relationsWithName.map(rn => {
        (GHD.getNumericalOrdering(attributeOrdering, rn), rn)
      }).groupBy(p => p._1).toList.map(elem => {
        (elem._1, elem._2.unzip._2)
      })

      orderingsAndRels.map(orderingAndRels => {
        val ordering = orderingAndRels._1
        val rels = orderingAndRels._2
        if (forTopLevelSummary) {
          Json(
            "name" -> jString(rels.head.name),
            "ordering" -> jArray(ordering.map(o => jNumber(o))),
            "annotation" -> jString(rels.head.annotationType)
          )
        } else {
          Json(
            "name" -> jString(rels.head.name),
            "ordering" -> jArray(ordering.map(o => jNumber(o))),
            "attributes" -> jArray(rels.map(rel => jArray(rel.attrNames.map(attrName => jString(attrName))))),
            "annotation" -> jString(rels.head.annotationType)
          )
        }
      })
    })
  }

  def createAttrToRelsMapping : Unit = {
    attrToRels = attrSet.map(attr =>{
      val relevantRels = subtreeRels.filter(rel => {
        rel.attrNames.contains(attr)
      })
      (attr, relevantRels)
    }).toMap
    children.map(child => child.createAttrToRelsMapping)
  }

  def setAttributeOrdering(ordering: List[Attr] ): Unit = {
    attributeOrdering = ordering
    rels = rels.map(rel => {
      new QueryRelation(rel.name, rel.attrs.sortWith((attrInfo1, attrInfo2) => {
        ordering.indexOf(attrInfo1._1) < ordering.indexOf(attrInfo2._1)
      }), rel.annotationType)
    })
    children.map(child => child.setAttributeOrdering(ordering))
  }

  /**
   * Compute what is projected out in this bag, and what this bag's output relation is
   */
  def computeProjectedOutAttrsAndOutputRelation(outputAttrs:Set[Attr], attrsFromAbove:Set[Attr]): QueryRelation = {
    projectedOutAttrs = attrSet -- (outputAttrs ++ attrsFromAbove)
    val keptAttrs = attrSet intersect (outputAttrs ++ attrsFromAbove)
    // Right now we only allow a query to have one type of annotation, so
    // we take the annotation type from an arbitrary relation that was joined in this bag
    outputRelation = new QueryRelation(bagName, keptAttrs.map(attr =>(attr, "", "")).toList, rels.head.annotationType)
    val childrensOutputRelations = children.map(child => {
      child.computeProjectedOutAttrsAndOutputRelation(outputAttrs, attrsFromAbove ++ attrSet)
    })
    subtreeRels ++= childrensOutputRelations
    return outputRelation
  }

  def computeDepth : Unit = {
    if (children.isEmpty) {
      depth = 0
    } else {
      val childrenDepths = children.map(x => {
        x.computeDepth
        x.depth
      })
      depth = childrenDepths.foldLeft(0)((acc:Int, x:Int) => {
        if (x > acc) x else acc
      })
    }
  }

  def getNumBags(): Int = {
    1 + children.foldLeft(0)((accum : Int, child : GHDNode) => accum + child.getNumBags())
  }

  def scoreTree(): Int = {
    bagWidth = attrSet.size
    return children.map((child: GHDNode) => child.scoreTree()).foldLeft(bagWidth)((accum: Int, x: Int) => if (x > accum) x else accum)
  }

  private def getMatrixRow(attr : String, rels : List[QueryRelation]): Array[Double] = {
    val presence = rels.map((rel : QueryRelation) => if (rel.attrNames.toSet.contains(attr)) 1.0 else 0)
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
}
