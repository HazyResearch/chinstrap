package DunceCap

import scala.collection.mutable
import java.io.{FileWriter, BufferedWriter, File}

object GHDSolver {
  def getAttrSet(rels: List[Relation]): Set[String] = {
    return rels.foldLeft(Set[String]())(
      (accum: Set[String], rel : Relation) => accum | rel.attrs.toSet[String])
  }
  private def breadth_first(seen: mutable.Set[GHDNode], f_in:mutable.Set[GHDNode]): (Int,Int) = {
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
    return (depth,seen.size)
  }
  def getGHD(distinctRelations:List[Relation]) : GHDNode = {
    val decompositions = getMinFHWDecompositions(distinctRelations) 
    //compute fractional scores
    val ordered_decomp = decompositions.sortBy{ root:GHDNode =>
      val tup = breadth_first(mutable.LinkedHashSet[GHDNode](root),mutable.LinkedHashSet[GHDNode](root))
      root.depth = tup._1
      root.depth
    }

    //pull out lowest depth FHWS 
    val myghd = if(Environment.yanna) ordered_decomp.head else getDecompositions(distinctRelations).last
    myghd.num_bags = breadth_first(mutable.LinkedHashSet[GHDNode](myghd),mutable.LinkedHashSet[GHDNode](myghd))._2
    assert(myghd.num_bags != 0)
    val fhws = myghd.fractionalScoreTree()
    return myghd
  }
  def getAttributeOrdering(myghd:GHDNode, resultAttrs:List[String]) : List[String] ={
    return resultAttrs
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








