package DunceCap

import scala.collection.mutable
import java.io.{FileWriter, BufferedWriter, File}

object GHDSolver {
  type EquivalenceClasses = (Map[(String,Int),(Int,Int,String,Int)],Map[String,Int],Map[Int,String],Map[String,String])
  type TopDown = (List[String],List[String],List[(List[(String,String,String,List[(String,String,String)])])])

  def getAttrSet(rels: List[Relation]): Set[String] = {
    return rels.foldLeft(Set[String]())(
      (accum: Set[String], rel : Relation) => accum | rel.attrs.toSet[String])
  }

  /**
   * relations is a list of tuples, where the first element is the name, and the second is the list of attributes
   */
  private def get_attribute_ordering(seen: mutable.Set[GHDNode], f_in:mutable.Set[GHDNode],resultAttrs:List[String]): List[String] = {
    //Runs a BFS, adds attributes in that order with the special condition that those attributes
    //that also exist in the children are added first, we also sort by the frequency of the attribute
    var depth = 0
    var frontier = f_in
    var next_frontier = mutable.Set[GHDNode]()
    var attr = scala.collection.mutable.ListBuffer.empty[String]

    while(frontier.size != 0){
      next_frontier.clear
      val level_attr = scala.collection.mutable.ListBuffer.empty[String]
      frontier.foreach{ cur:GHDNode =>
        //first add attributes with elements in common with children, then add others        
        val children_attrs = cur.children.flatMap{ c => c.rels.flatMap{r => r.attrs}.toList}
        //sort by frequency
        val children_attrs_sorted = children_attrs.distinct.sortBy(children_attrs count _.==).reverse
        val cur_attrs = cur.rels.flatMap{r => r.attrs}

        //find shared attributes and sort by frequency
        val shared_attrs = cur_attrs.intersect(children_attrs_sorted).sortBy(e => children_attrs_sorted.indexOf(e))
        //shared attributes added first. Should be added in order of how many times
        //appears in the child.
        shared_attrs.foreach{ a =>
          if(!attr.contains(a)){
            attr += a
          }
        }
        //collect others
        cur_attrs.foreach{ a =>
          if(!attr.contains(a) && !level_attr.contains(a)){
            level_attr += a
          }
        }

        cur.children.foreach{(child:GHDNode) =>
          if(!seen.contains(child)){
            seen += child
            next_frontier += child
          }
        }
      }

      val cur_attrs_sorted = level_attr.sortBy(e => if(resultAttrs.contains(e)) resultAttrs.indexOf(e) else resultAttrs.size+1)
      cur_attrs_sorted.foreach{ a =>
        if(!attr.contains(a)){
          attr += a
        }
      }

      var tmp = frontier
      frontier = next_frontier
      next_frontier = tmp
      
      depth += 1
    }
    return attr.toList
  }

  def bottom_up(seen: mutable.Set[GHDNode], curr: GHDNode, fn:(CodeStringBuilder,GHDNode,List[String],List[List[ASTCriterion]],Boolean,EquivalenceClasses,List[String]) => Unit, s:CodeStringBuilder, attribute_ordering:List[String], selections:List[List[ASTCriterion]], aggregate:Boolean, equivalenceClasses:EquivalenceClasses, resultAttrs:List[String]): Unit = {
    for (child <- curr.children) {
      if (!seen.contains(child)) {
        seen += child
        bottom_up(seen, child, fn, s, attribute_ordering, selections,aggregate,equivalenceClasses,resultAttrs)
      }
    }
    val bag_attrs = curr.rels.flatMap(r => r.attrs).toList.distinct
    val a_i = attribute_ordering.zipWithIndex.filter(a => bag_attrs.contains(a._1))
    val s_in = a_i.map{ i => selections(i._2) }
    fn(s,curr,a_i.map(_._1),s_in,aggregate,equivalenceClasses,resultAttrs)
  }

  private def top_down_PRE(seen: mutable.Set[GHDNode], curr: GHDNode, resultAttrs:List[String]): List[String] = {
    val newAttrs = curr.attribute_ordering.filter(resultAttrs.contains(_))
    val result = mutable.ListBuffer[String]()
    if(newAttrs.size != 0){
      result += curr.name + "_block"
    }

    curr.children.foreach { child =>
      // if these two hyperedges are connected
      if (!seen.contains(child)) {
        seen += child
        result ++= top_down_PRE(seen, child, resultAttrs)
      }
    }
    return result.toList
  }

  private def top_down_DFS(seen: mutable.Set[GHDNode], curr: GHDNode, search_attribute:String, resultAttrs:List[String], f:(Int => Int)): List[(String,String,String)] = {
    val newAttrs = curr.attribute_ordering.filter(resultAttrs.contains(_))
    val i = f(newAttrs.indexOf(search_attribute))

    val get_string = if(i != 0 && (i < newAttrs.size) ) "get_block(" + search_attribute + "_d)"  else ""
    val prev_block_string = if(i > 1 && (i < newAttrs.size)) "_" + newAttrs(i-1) else ""
    val iterator_string = if(i > 1 && (i < newAttrs.size)) "_iterator." else "->"
    val next_attr = if( i < newAttrs.size) newAttrs(i) else ""
    val access_string = if(i < newAttrs.size) (curr.name + prev_block_string + "_block" + iterator_string + get_string) else ""

    val result = if(i < newAttrs.size) mutable.ListBuffer[(String,String,String)]( (next_attr,curr.name,access_string) ) else mutable.ListBuffer[(String,String,String)]()
    curr.children.foreach { child =>
      // if these two hyperedges are connected
      if (!seen.contains(child) && child.attribute_ordering.contains(search_attribute)) {
        seen += child
        result ++= top_down_DFS(seen, child, search_attribute, resultAttrs,f)
      }
    }
    return result.toList
  }

  def top_down(seen: mutable.Set[GHDNode], f_in:mutable.Set[GHDNode], resultAttrs:List[String]): TopDown = {
    var depth = 0
    var frontier = f_in
    var next_frontier = mutable.Set[GHDNode]()
    val visited_attributes = mutable.Set[String]()
    val result_attributes = mutable.ListBuffer[String]()

    // Map [ Trie ->  List[attribute,check] ]
    val result = mutable.ListBuffer[(List[(String,String,String,List[(String,String,String)])])]()

    var final_accessor = mutable.Map[String,String]()
    var final_checks = mutable.Map[String,mutable.Set[String]]()

    //Lets make sure the tries actually exist before we run the top down pass
    val preChecks = top_down_PRE(mutable.LinkedHashSet[GHDNode](seen.head), seen.head, resultAttrs)

    while(frontier.size != 0){
      next_frontier.clear

      frontier.foreach{ cur:GHDNode =>
        val cur_result = mutable.ListBuffer[(String,String,String,List[(String,String,String)])]()
        val newAttrs = cur.attribute_ordering.filter(resultAttrs.contains(_))
        result_attributes ++= newAttrs.filter(na => !result_attributes.contains(na))
        var prev_access = ""
        (0 until newAttrs.size).foreach{i =>
          var access_string = ""
          val a = newAttrs(i)
          if(!visited_attributes.contains(newAttrs(i))){
            visited_attributes += a
            val get_string = if(i != 0) "->get_block"+"("+ newAttrs(i-1) + "_i," + newAttrs(i-1) + "_d)"  else ""
            val prev_block_string = if(i > 1) "_" + newAttrs(i-1) else ""
            println(i + " " + newAttrs)
            access_string = cur.name + prev_block_string + "_block" + get_string
          }
          
          //dfs to find other tries that need to be added for check.
          val checks = mutable.ListBuffer[(String,String,String)]();
          cur.children.foreach{(child:GHDNode) =>
            if(!seen.contains(child)){
              seen += child
              next_frontier += child
            }
            if(child.attribute_ordering.contains(a)){
              //run DFS
              checks ++= top_down_DFS(mutable.LinkedHashSet[GHDNode](child), child, a, resultAttrs, i => i+1)
            }
          }
          prev_access = access_string
          cur_result += ((cur.name,access_string,a,checks.toList))

        }
        println("HERE")
        cur_result.foreach{println}
        result += cur_result.toList
      }
      var tmp = frontier
      frontier = next_frontier
      next_frontier = tmp

      depth += 1
    }

    return (preChecks,result_attributes.toList,result.toList)
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
    print(myghd, "query_plan_" + fhws + ".json")
    return myghd
  }
  def getAttributeOrdering(myghd:GHDNode, resultAttrs:List[String]) : List[String] ={
    val attribute_ordering = get_attribute_ordering(mutable.LinkedHashSet[GHDNode](myghd),mutable.LinkedHashSet[GHDNode](myghd),resultAttrs)
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








