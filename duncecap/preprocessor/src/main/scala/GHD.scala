package DunceCap

import java.util

import DunceCap.attr.Attr
import argonaut.Argonaut._
import argonaut.Json
import org.apache.commons.math3.optim.linear._
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType

import scala.collection.immutable.TreeSet
;


object GHD {
  def getNumericalOrdering(attributeOrdering:List[Attr], rel:QueryRelation): List[Int] = {
    attributeOrdering.map(a => rel.attrNames.indexOf(a)).filter(pos => {
      pos != -1
    })
  }
}

class GHD(val root:GHDNode, val queryRelations: List[QueryRelation], val outputRelation: QueryRelation) {
  val attributeOrdering: List[Attr] = GHDSolver.getAttributeOrdering(root, outputRelation.attrNames)
  var depth: Int = -1
  var numBags: Int = -1

  def toJson(): Json = {
    /**
     *
    "query_type":"join",
    "relations": [
		{
			"name":"R",
			"ordering":[0,1],
			"annotation":"void*"
		}
	  ],
     **/
    // do a preorder traversal of the GHD and get the info for all the bags
    Json("ghd" -> jArray(getJsonFromPreOrderTraversal(root)))
  }

  private def getRelationsSummary(): Json = ???

  private def getRelationSummaryFromPreOrderTraversal() = ???

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
      "ordering" -> jString(GHD.getNumericalOrdering(attributeOrdering, outputRelation).toString),
      "annotation" -> jString(outputRelation.annotationType)
    )
  }

  private def getJsonFromPreOrderTraversal(node:GHDNode): List[Json] = {
    node.getJsonBagInfo::node.children.flatMap(c => getJsonFromPreOrderTraversal(c))
  }

  def doPostProcessingPass() = {
    root.computeDepth
    depth = root.depth
    numBags = root.getNumBags()
    root.setAttributeOrdering(attributeOrdering)
    root.computeProjectedOutAttrsAndOutputRelation(outputRelation.attrNames.toSet, Set())
  }
}


class GHDNode(var rels: List[QueryRelation]) {
  val attrSet = rels.foldLeft(TreeSet[String]())(
    (accum: TreeSet[String], rel: QueryRelation) => accum | TreeSet[String](rel.attrNames: _*))
  var attrToRels:Map[Attr,List[QueryRelation]] = null
  var attributeOrdering: List[Attr] = null
  var children: List[GHDNode] = List()
  var bagFractionalWidth: Double = 0
  var bagWidth: Int = 0
  var depth: Int = 0
  var projectedOutAttrs: Set[Attr] = null
  var outputRelation: QueryRelation = null

  override def equals(o: Any) = o match {
    case that: GHDNode => that.rels.equals(rels) && that.children.equals(children)
    case _ => false
  }

  def getNumericalOrdering(rel:QueryRelation): List[Int] = {
    attributeOrdering.map(a => rel.attrNames.indexOf(a)).filter(pos => {
      pos != -1
    })
  }

  override def hashCode = 41 * rels.hashCode() + children.hashCode()

  def getJsonBagInfo(): Json = {
    val jsonRelInfo = getJsonRelationInfo()
    Json(
      "name" -> jString("bag" + hashCode),
      "attributes" -> jString(outputRelation.attrNames.toString),
      "annotation" -> jString(outputRelation.annotationType),
      "relations" -> jArray(jsonRelInfo),
      "nprr" -> jString("TODO!!!!")
    )
  }

  private def getJsonNPRRInfo() = ???

  /**
   * Generates the following:
   *
  "relations": [
        {
          "name":"R",
          "ordering":[0,1],
          "attributes":[["a","b"],["b","c"],["a","c"]],
          "annotation":"void*"
        }
      ],
   */
  private def getJsonRelationInfo(): List[Json] = {
    val distinctRelationNames = rels.map(r => r.name).distinct
    distinctRelationNames.flatMap(n => {
      val relationsWithName = rels.filter(r => {r.name == n})
      val orderingsAndRels: List[(List[Int], List[QueryRelation])] = relationsWithName.map(rn => {
        (getNumericalOrdering(rn), rn)
      }).groupBy(p => p._1).toList.map(elem => {
        (elem._1, elem._2.unzip._2)
      })

      orderingsAndRels.map(orderingAndRels => {
        val ordering = orderingAndRels._1
        val rels = orderingAndRels._2
        Json(
          "name" -> jString(rels.head.name),
          "ordering" -> jString(ordering.toString),
          "attributes" -> jArray(rels.map(rel => jString(rel.attrNames.toString))),
          "annotation" -> jString(rels.head.annotationType)
        )
      })
    })
  }

  def setAttributeOrdering(ordering: List[Attr] ): Unit = {
    attributeOrdering = ordering
    rels = rels.map(rel => {
      new QueryRelation(rel.name, rel.attrs.sortWith((attrInfo1, attrInfo2) => {
        ordering.indexOf(attrInfo1._1) < ordering.indexOf(attrInfo2._1)
      }), rel.annotationType)
    })

    children.map(child => child.setAttributeOrdering(ordering))

    attrToRels = attrSet.map(attr =>{
      val relevantRels = rels.filter(rel => {
        rel.attrNames.contains(attr)
      })
      (attr, relevantRels)
    }).toMap
  }

  /**
   * Compute what is projected out in this bag, and what this bag's output relation is
   */
  def computeProjectedOutAttrsAndOutputRelation(outputAttrs:Set[Attr], attrsFromAbove:Set[Attr]): Unit = {
    projectedOutAttrs = attrSet -- (outputAttrs ++ attrsFromAbove)
    val keptAttrs = attrSet intersect (outputAttrs ++ attrsFromAbove)
    // Right now we only allow a query to have one type of annotation, so
    // we take the annotation type from an arbitrary relation that was joined in this bag
    outputRelation = new QueryRelation("", keptAttrs.map(attr =>(attr, "", "")).toList, rels.head.annotationType)
    children.map(child => {
      child.computeProjectedOutAttrsAndOutputRelation(outputAttrs, attrsFromAbove ++ attrSet)
    })
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

  def getName(attribute_ordering: List[String]): String = {
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

  def toJson(): Json = ???

}
