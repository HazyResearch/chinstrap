package DunceCap

import scala.collection.mutable
import java.io.{FileWriter, BufferedWriter, File}

object GHDSolver {
  type EquivalenceClasses = (Map[(String,Int),(Int,Int,String)],Map[String,Int],Map[Int,String])

  def getAttrSet(rels: List[Relation]): Set[String] = {
    return rels.foldLeft(Set[String]())(
      (accum: Set[String], rel : Relation) => accum | rel.attrs.toSet[String])
  }

  /**
   * relations is a list of tuples, where the first element is the name, and the second is the list of attributes
   */
  private def get_attribute_ordering(seen: mutable.Set[GHDNode], f_in:mutable.Set[GHDNode]): List[String] = {
    //Runs a BFS, adds attributes in that order with the special condition that those attributes
    //that also exist in the children are added first
    var depth = 0
    var frontier = f_in
    var next_frontier = mutable.Set[GHDNode]()
    var attr = scala.collection.mutable.ListBuffer.empty[String]
    while(frontier.size != 0){
      next_frontier.clear
      frontier.foreach{ cur:GHDNode =>

        //first add attributes with elements in common with children, then add others        
        val children_attrs = cur.children.flatMap{ c => c.rels.flatMap{r => r.attrs}.toList.distinct}
        val cur_attrs = cur.rels.flatMap{r => r.attrs}
        children_attrs.intersect(cur_attrs).foreach{ a =>
          if(!attr.contains(a)){
            attr += a
          }
        }
        //add others
        cur_attrs.foreach{ a =>
          if(!attr.contains(a)){
            attr += a
          }
        }

        cur.children.foreach{(child:GHDNode) =>
          if(!seen.contains(child)){
            seen += child
            next_frontier += child
          }
        }
      }

      var tmp = frontier
      frontier = next_frontier
      next_frontier = tmp
      
      depth += 1
    }
    return attr.toList
  }

  def bottom_up(seen: mutable.Set[GHDNode], curr: GHDNode, fn:(CodeStringBuilder,GHDNode,List[String],List[List[ASTCriterion]],Boolean,EquivalenceClasses) => Unit, s:CodeStringBuilder, attribute_ordering:List[String], selections:List[List[ASTCriterion]], aggregate:Boolean, equivalenceClasses:EquivalenceClasses): Unit = {
    for (child <- curr.children) {
      if (!seen.contains(child)) {
        seen += child
        bottom_up(seen, child, fn, s, attribute_ordering, selections,aggregate,equivalenceClasses)
      }
    }
    val bag_attrs = curr.rels.flatMap(r => r.attrs).toList.distinct
    val a_i = attribute_ordering.zipWithIndex.filter(a => bag_attrs.contains(a._1))
    val s_in = a_i.map{ i => selections(i._2) }
    fn(s,curr,a_i.map(_._1),selections,aggregate,equivalenceClasses)
  }
  def top_down(seen: mutable.Set[GHDNode], f_in:mutable.Set[GHDNode]): (Map[String,String],Map[String,mutable.Set[String]]) = {
    var depth = 0
    var frontier = f_in
    var next_frontier = mutable.Set[GHDNode]()

    var visited_attributes = mutable.Set[String]()
    var final_accessor = mutable.Map[String,String]()
    var final_checks = mutable.Map[String,mutable.Set[String]]()

    while(frontier.size != 0){
      next_frontier.clear

      frontier.foreach{ cur:GHDNode =>

        (0 until cur.attribute_ordering.size).foreach{i =>
          if(!visited_attributes.contains(cur.attribute_ordering(i))){
            visited_attributes += cur.attribute_ordering(i)
            val a1 = cur.attribute_ordering(i)
            val a2 = cur.name + "_block" + (0 until i).map{s =>
              "->get_block(" + cur.attribute_ordering(s) + "_d)" 
            }.mkString("")
            final_accessor += ((a1,a2))
          }
          if((i+1) < cur.attribute_ordering.size){
            if(!final_checks.contains(cur.attribute_ordering(i))){
              final_checks += ((cur.attribute_ordering(i),mutable.Set[String]()))
            }
            final_checks(cur.attribute_ordering(i)) += cur.attribute_ordering(i+1)
          }
        }

        cur.children.foreach{(child:GHDNode) =>
          var name = ""
          (0 until 1).foreach{ i =>
            //can only be first level...past that we have a dependency
            if((i+1) < child.attribute_ordering.size){
              if(!final_checks.contains(child.attribute_ordering(i))){
                final_checks += ((child.attribute_ordering(i),mutable.Set[String]()))
              }
              final_checks(child.attribute_ordering(i)) += child.attribute_ordering(i+1)
            }
          }
          if(!seen.contains(child)){
            seen += child
            next_frontier += child
          }
        }
      }

      var tmp = frontier
      frontier = next_frontier
      next_frontier = tmp

      depth += 1
    }

    final_accessor.foreach{println}
    println("FINAL CHECKS")
    final_checks.foreach{println}
    (final_accessor.toMap,final_checks.toMap)
  }
  private def breadth_first(seen: mutable.Set[GHDNode], f_in:mutable.Set[GHDNode]): Int = {
    var depth = 0
    var frontier = f_in
    var next_frontier = mutable.Set[GHDNode]()
    while(frontier.size != 0){
      next_frontier.clear
      frontier.foreach{ cur:GHDNode =>
        cur.children.foreach{(child:GHDNode) =>
          if(!seen.contains(child)){
            seen += child
            next_frontier += child
          }
        }
      }

      var tmp = frontier
      frontier = next_frontier
      next_frontier = tmp

      depth += 1
    }
    return depth
  }
  def getGHD(distinctRelations:List[Relation]) : GHDNode = {
    val decompositions = getMinFHWDecompositions(distinctRelations) 
    //compute fractional scores
    val ordered_decomp = decompositions.sortBy{ root:GHDNode =>
      breadth_first(mutable.LinkedHashSet[GHDNode](root),mutable.LinkedHashSet[GHDNode](root))
    }
    //pull out lowest depth FHWS 
    val myghd = ordered_decomp.head
    val fhws = myghd.fractionalScoreTree()
    print(myghd, "query_plan_" + fhws + ".json")
    return myghd
  }
  def getAttributeOrdering(myghd:GHDNode) : List[String] ={
    val attribute_ordering = get_attribute_ordering(mutable.LinkedHashSet[GHDNode](myghd),mutable.LinkedHashSet[GHDNode](myghd))
    println("Attribute Ordering")
    attribute_ordering.foreach{println}
    println("Attribute Ordering")
    return attribute_ordering
  }

  private def getConnectedComponents(rels: mutable.Set[Relation], comps: List[List[Relation]], ignoreAttrs: Set[String]): List[List[Relation]] = {
    if (rels.isEmpty) return comps
    val component = getOneConnectedComponent(rels, ignoreAttrs)
    return getConnectedComponents(rels, component::comps, ignoreAttrs)
  }

  private def getOneConnectedComponent(rels: mutable.Set[Relation], ignoreAttrs: Set[String]): List[Relation] = {
    val curr = rels.head
    rels -= curr
    return DFS(mutable.LinkedHashSet[Relation](curr), curr, rels, ignoreAttrs)
  }

  private def DFS(seen: mutable.Set[Relation], curr: Relation, rels: mutable.Set[Relation], ignoreAttrs: Set[String]): List[Relation] = {
    for (rel <- rels) {
      // if these two hyperedges are connected
      if (!((curr.attrs.toSet[String] & rel.attrs.toSet[String]) &~ ignoreAttrs).isEmpty) {
        seen += curr
        rels -= curr
        DFS(seen, rel, rels, ignoreAttrs)
      }
    }
    return seen.toList
  }

  // Visible for testing
  def getPartitions(leftoverBags: List[Relation], // this cannot contain chosen
                    chosen: List[Relation],
                    parentAttrs: Set[String],
                    tryBagAttrSet: Set[String]): Option[List[List[Relation]]] = {
    // first we need to check that we will still be able to satisfy
    // the concordance condition in the rest of the subtree
    for (bag <- leftoverBags.toList) {
      if (!(bag.attrs.toSet[String] & parentAttrs).subsetOf(tryBagAttrSet)) {
        return None
      }
    }

    // if the concordance condition is satisfied, figure out what components you just
    // partitioned your graph into, and do ghd on each of those disconnected components
    val relations = mutable.LinkedHashSet[Relation]() ++ leftoverBags
    return Some(getConnectedComponents(relations, List[List[Relation]](), getAttrSet(chosen).toSet[String]))
  }

  /**
   * @param partitions
   * @param parentAttrs
   * @return Each list in the returned list could be the children of the parent that we got parentAttrs from
   */
  private def getListsOfPossibleSubtrees(partitions: List[List[Relation]], parentAttrs: Set[String]): List[List[GHDNode]] = {
    assert(!partitions.isEmpty)
    val subtreesPerPartition: List[List[GHDNode]] = partitions.map((l: List[Relation]) => getDecompositions(l, parentAttrs))

    val foldFunc: (List[List[GHDNode]], List[GHDNode]) => List[List[GHDNode]]
    = (accum: List[List[GHDNode]], subtreesForOnePartition: List[GHDNode]) => {
      accum.map((children : List[GHDNode]) => {
        subtreesForOnePartition.map((subtree : GHDNode) => {
          subtree::children
        })
      }).flatten
    }

    return subtreesPerPartition.foldLeft(List[List[GHDNode]](List[GHDNode]()))(foldFunc)
  }

  private def getDecompositions(rels: List[Relation], parentAttrs: Set[String]): List[GHDNode] =  {
    val treesFound = mutable.ListBuffer[GHDNode]()
    for (tryNumRelationsTogether <- (1 to rels.size).toList) {
      for (bag <- rels.combinations(tryNumRelationsTogether).toList) {
        val leftoverBags = rels.toSet[Relation] &~ bag.toSet[Relation]
        if (leftoverBags.toList.isEmpty) {
          val newNode = new GHDNode(bag)
          treesFound.append(newNode)
        } else {
          val bagAttrSet = getAttrSet(bag)
          val partitions = getPartitions(leftoverBags.toList, bag, parentAttrs, bagAttrSet)
          if (partitions.isDefined) {
            // lists of possible children for |bag|
            val possibleSubtrees: List[List[GHDNode]] = getListsOfPossibleSubtrees(partitions.get, bagAttrSet)
            for (subtrees <- possibleSubtrees) {
              val newNode = new GHDNode(bag)
              newNode.children = subtrees
              treesFound.append(newNode)
            }
          }
        }
      }
    }
    return treesFound.toList
  }

  def getDecompositions(rels: List[Relation]): List[GHDNode] = {
    return getDecompositions(rels, Set[String]())
  }

  def getMinFHWDecompositions(rels: List[Relation]): List[GHDNode] = {
    val decomps = getDecompositions(rels)
    val fhwsAndDecomps = decomps.map((root : GHDNode) => (root.fractionalScoreTree(), root))
    val minScore = fhwsAndDecomps.unzip._1.min

    case class Precision(val p:Double)
    class withAlmostEquals(d:Double) {
      def ~=(d2:Double)(implicit p:Precision) = (d-d2).abs <= p.p
    }
    implicit def add_~=(d:Double) = new withAlmostEquals(d)
    implicit val precision = Precision(0.001)

    val minFhws = fhwsAndDecomps.filter((scoreAndNode : (Double, GHDNode)) => scoreAndNode._1 ~= minScore)
    return minFhws.unzip._2
  }

  def print(root: GHDNode, filename: String) = {
    val json = root.toJson()
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.append(json.spaces2)
    bw.close()
  }
}








