package DunceCap

import scala.collection.mutable

/**
 * All code generation should start from this object:
 */
object CodeGen {

  /**
   * This following method will get called in both compiler and repl mode
   */
  def emitHeaderAndCodeForAST(s: CodeStringBuilder, root: ASTNode) = {
    s.println("#include \"emptyheaded.hpp\"")
    s.println("extern \"C\" void run(std::unordered_map<std::string, void*>& relations) {")
    root.code(s)
    s.println("}")
  }

  /**
    * emitMainMethod will get called only in compiler mode
    */
  def emitMainMethod(s : CodeStringBuilder): Unit = {
    s.println("int main() {")
    s.println("std::unordered_map<std::string, void*> relations;")
    s.println("run(relations);")
    s.println("}")
  }
}


/**
 * Explanation of types below:
 *
 * Every node in the AST is an ASTNode. Some of these are ASTStatements,
 * others are ASTExpressions.
 *
 * All ASTNodes emit code, and have an optimize method that might transform the AST rooted at that node.
 * Since ASTStatements can have side effects, these have an updateEnvironment method.
 */
abstract trait ASTNode {
  val layout = "hybrid"
  def code(s: CodeStringBuilder)
  def optimize
}

case class ASTStatements(statements : List[ASTStatement]) extends ASTNode {
  def code(s: CodeStringBuilder): Unit = {
    statements.map((statement : ASTStatement) => {
      s.println(s"""////////////////////////////////////////////////////////////////////////////////""")
      s.println("{")
      statement.code(s)
      s.println("}")
      statement.updateEnvironment
    })
  }

  override def optimize: Unit = ???
}

abstract trait ASTStatement extends ASTNode {
  def updateEnvironment = {}
}

case class ASTLoadFileStatement(rel : ASTRelation, filename : ASTStringLiteral, format : String) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    val relationTypes = rel.attrs.mkString(",")
    s.println(s"Relation<${relationTypes}>* ${rel.identifierName} = new Relation<${relationTypes}>();")
    assert(format == "tsv") // TODO : add in csv option
    s.println(s"""tsv_reader f_reader("${(filename.str)}");""")
    s.println("char *next = f_reader.tsv_get_first();")
    s.println(s" ${rel.identifierName}->num_rows = 0;")
    s.println("while(next != NULL){")
    for (i <- (0 to (rel.attrs.size-1)).toList) {
      s.println(s"${rel.identifierName}->get<${i}>().append_from_string(next);")
      s.println("next = f_reader.tsv_get_next();")
    }
    s.println(s"${rel.identifierName}->num_rows++;")
    s.println("}")

    // make sure to add the relation and encoding we've just made to the maps the server keeps track of
    s.println(s"""relations["${rel.identifierName}"] = ${rel.identifierName};""")

    s.println(s"""std::cout << ${rel.identifierName}->num_rows << " rows loaded." << std::endl;""")
  }

  override def updateEnvironment: Unit = {
    Environment.addRelationBinding(rel.identifierName, rel.attrs)
  }

  override def optimize: Unit = ???
}

case class ASTAssignStatement(identifier : ASTIdentifier, expression : ASTExpression) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    expression.code(s)
  }

  override def optimize: Unit = ???
}

case class ASTPrintStatement(expression : ASTExpression) extends ASTNode with ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    expression match {
      case ASTScalar(identifierName) => {
        val typeString = Environment.getTypes(identifierName).mkString(", ")
        s.println(s"""Relation<${typeString}> * ${identifierName}= (Relation<${typeString}> *)relations["${identifierName}"];""")
        s.println(s"""std::cout << "${identifierName} has " << ${identifierName}->num_rows << " rows loaded." << std::endl;""")
      }
      case _ => println(expression)
    }
  }

  override def optimize: Unit = ???
}

abstract trait ASTExpression extends ASTNode
case class ASTCount(expression : ASTExpression) extends ASTExpression {
  override def code(s: CodeStringBuilder): Unit = ???

  override def optimize: Unit = ???
}

case class ASTJoinAndSelect(rels : List[ASTRelation], selectCriteria : List[ASTCriterion]) extends ASTExpression {

  import NPRRSetupUtil._

  private def emitRelationLookupAndCast(s: CodeStringBuilder, rels : List[String]) = {
    rels.map((identifierName : String) => {
      val typeString = Environment.getTypes(identifierName).mkString(", ")
      s.println(s"""Relation<${typeString}> * ${identifierName}= (Relation<${typeString}> *)relations["${identifierName}"];""")
    })
  }

  private def emitSelectionCondition(s:CodeStringBuilder, sel:ASTCriterion){
    (sel.attr1,sel.attr2) match{
      case (a:ASTScalar,b:ASTStringLiteral) =>
        a.code(s)
        s.print("_data == ")
        a.code(s)
        s.print("_selection")
      case (a:ASTStringLiteral,b:ASTScalar) =>
        b.code(s)
        s.print("_data == ")
        b.code(s)
        s.print("_selection")
    }
  }

  private def emitIntersectionSelection(s: CodeStringBuilder, attr:String, sel:List[ASTCriterion]): Unit = {
    if(sel.size != 0){
      s.print(s""",[&](uint32_t ${attr}_data, uint32_t _1, uint32_t _2){ return """)
      emitSelectionCondition(s,sel.head)
      sel.tail.foreach{ i =>
        s.print(" && ")
        emitSelectionCondition(s,i)
      }
      s.print(";})")
    }
  }

  private def emitAttrIntersection(s: CodeStringBuilder, lastIntersection : Boolean, attr : String, sel: List[ASTCriterion], relsAttrs :  List[(String, List[String])], aggregate:Boolean, equivalenceClasses:EquivalenceClasses, prev_a:String, prev_t:String, name:String) : List[(String, List[String])]= {
    val relsAttrsWithAttr = relsAttrs.filter(( rel : (String, List[String])) => rel._2.contains(attr)).unzip._1.distinct
    assert(!relsAttrsWithAttr.isEmpty)
    val (encodingMap,attrMap,encodingNames) = equivalenceClasses
    val encodingName = encodingNames(attrMap(attr))

    var tid = "tid"
    if(prev_a == "")
      tid = "0"

    if (relsAttrsWithAttr.size == 1) {
      // no need to emit an intersection
      s.println( s"""Set<${layout}> ${attr} = ${relsAttrsWithAttr.head}->set;""")
      if(!aggregate){
        if(prev_a == ""){
          s.println(s"""${name}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout}>))) TrieBlock<${layout}>(${relsAttrsWithAttr.head});""")
          s.println(s"""${name}_block->init_pointers(${tid},&output_buffer,${encodingName}_encoding.num_distinct);""")      
        }
        else{
          s.println(s"""TrieBlock<${layout}> *${attr}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout}>))) TrieBlock<${layout}>(${relsAttrsWithAttr.head});""")
          s.println(s"""${attr}_block->init_pointers(${tid},&output_buffer,${encodingName}_encoding.num_distinct);""")      
        }
      }
     } else {
      if(!aggregate){
        if(prev_a == "")
          s.println(s"""${name}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout}>))) TrieBlock(true);""")
        else
          s.println(s"""TrieBlock<${layout}> *${attr}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout}>))) TrieBlock<${layout}>(true);""")
      }
      s.println(s"""const size_t alloc_size = sizeof(uint64_t)*${encodingName}_encoding.num_distinct*2;""")
      s.println(s"""Set<${layout}> ${attr}(output_buffer.get_next(${tid},alloc_size)); //initialize the memory""")
      s.print(s"""${attr} = *ops::set_intersect(&${attr},(const Set<${layout}> *)&${relsAttrsWithAttr.head}->set,(const Set<${layout}> *)&${relsAttrsWithAttr.tail.head}->set""")

      emitIntersectionSelection(s,attr,sel)
      s.println(");")

      
      //have buffers you switch between for intersecting the rest (FIXME can leak memory)
      val restOfRelsAttrsWithAttr = relsAttrsWithAttr.tail.tail 
      if(restOfRelsAttrsWithAttr.size != 0)
        s.println(s"""Set<${layout}> ${attr}_tmp(output_buffer.get_next(${tid},alloc_size)); //initialize the memory""")
      var tmp = false
      restOfRelsAttrsWithAttr.foreach{(rel : String) => 
        if(!tmp){
          s.print(s"""${attr}_tmp = *ops::set_intersect(&${attr},&${attr},&${rel}->set""")
        }
        else{
          s.print(s"""${attr} = *ops::set_intersect(&${attr}_tmp,&${attr},&${rel}->set""")
        }

        emitIntersectionSelection(s,attr,sel)
        s.println(");")
      }
      if(restOfRelsAttrsWithAttr.size != 0){
        if(tmp)
          s.println(s"""output_buffer.roll_back(${tid},alloc_size);""")
        else
          s.println(s"""${attr} = ${attr}_tmp;""")
      }

      s.println(s"""output_buffer.roll_back(${tid},alloc_size - ${attr}.number_of_bytes);""")
      if(!aggregate){
        s.println(s"""${attr}_block->init_pointers(${tid},&output_buffer,${encodingName}_encoding.num_distinct);""")
        s.println(s"""${attr}_block->set= &${attr};""")
        if(prev_a != "")
          s.println(s"""${prev_t}_block->set_block(${prev_a}_i,${prev_a}_d,${attr}_block);""")      
      }
    }

    if (lastIntersection) {
      s.println(s"""const size_t count = ${attr}.cardinality;""")
      s.println(s"""output_cardinality.update(${tid},count);""")
      List[(String, List[String])]()
    } else {
      // update relsAttrs by peeling attr off the attribute lists, and adding ->map.at(attr_i) to the relation name
      relsAttrs.map((rel : (String, List[String])) => {
        if (!rel._2.isEmpty && rel._2.head.contains(attr)) {
          (rel._1 + s"->get_block(${attr}_d)", rel._2.tail)
        } else {
          rel
        }
      })
    }
  }

  private def emitAttrLoopOverResult(s: CodeStringBuilder, outermostLoop : Boolean, attrs : List[String], relsAttrs : List[(String, List[String])], attrSelections:List[List[ASTCriterion]], aggregate:Boolean, equivalenceClasses:EquivalenceClasses, prev_a:String, prev_t:String, name:String) = {
    // this should include the walking down the trie, so that when you recursively call emitNPRR, you do so with different rel names
    if (outermostLoop) {
      if(aggregate)
        s.println(s"""${attrs.head}.par_foreach([&](size_t tid, uint32_t ${attrs.head}_d){""")
      else 
        s.println(s"""${attrs.head}.par_foreach_index([&](size_t tid, uint32_t ${attrs.head}_i, uint32_t ${attrs.head}_d){""")
      emitNPRR(s, false, attrs.tail, relsAttrs,attrSelections.tail,aggregate,equivalenceClasses,prev_a,prev_t,name)
      s.println("});")
    } else {
      if(aggregate)
        s.println(s"""${attrs.head}.foreach([&](uint32_t ${attrs.head}_d) {""")
      else
        s.println(s"""${attrs.head}.foreach_index([&](uint32_t ${attrs.head}_i, uint32_t ${attrs.head}_d) {""")
      emitNPRR(s, false, attrs.tail, relsAttrs,attrSelections.tail,aggregate,equivalenceClasses,prev_a,prev_t,name)
      s.println("});")
    }
  }

  private def emitNPRR(s: CodeStringBuilder, initialCall : Boolean, attrs : List[String], relsAttrs : List[(String, List[String])], attrSelections:List[List[ASTCriterion]], aggregate:Boolean, equivalenceClasses:EquivalenceClasses, prev_a:String, prev_t:String, name:String) : Unit = {
    if (attrs.isEmpty) return

    val currAttr = attrs.head
    if (attrs.tail.isEmpty) {
      emitAttrIntersection(s, true, currAttr, attrSelections.head, relsAttrs, aggregate, equivalenceClasses,prev_a,prev_t,name)
    } else {
      val updatedRelsAttrs = emitAttrIntersection(s, false, currAttr, attrSelections.head, relsAttrs, aggregate, equivalenceClasses,prev_a,prev_t,name)
      val pt = if(prev_a == "") name else currAttr
      emitAttrLoopOverResult(s, initialCall, attrs, updatedRelsAttrs, attrSelections, aggregate,equivalenceClasses,currAttr,pt,name)
    }
  }

  private def emitAttrEncoding(s : CodeStringBuilder, rewrittenAttrName : String, relName: String, attrIndex: Int): Unit = {
    s.println( s"""${rewrittenAttrName}_attributes->push_back(${relName}->get<${attrIndex}>()); """)
  }

  private def emitEncodingInitForAttr(s: CodeStringBuilder, attr : String, attrType: String) = {
    s.println(s"""std::vector<Column<${attrType}>> *${attr}_attributes = new std::vector<Column<${attrType}>>();""")
  }

  private def emitBuildEncodingForAttr(s: CodeStringBuilder,  attr : String, attrType: String) = {
    s.println(s"""Encoding<${attrType}> ${attr}_encoding(${attr}_attributes); // TODO heap allocate""")
  }

  private def emitEncodingRelations(s:CodeStringBuilder, rel:Relation, encodingMap:EM, encodingNames:Map[Int,String]) = {
    s.println(s"""std::vector<Column<uint32_t>> *E${rel.name} = new std::vector<Column<uint32_t>>();""")
    s.println(s"""std::vector<size_t> *ranges_${rel.name} = new std::vector<size_t>();""")

    (0 until rel.attrs.size).toList.foreach{ i =>
      val eID = encodingMap((rel.name,i))
      val e_name = encodingNames(eID._1)
      val e_index = eID._2
      s.println(s"""E${rel.name}->push_back(${e_name}_encoding.encoded.at(${e_index}));""")
      s.println(s"""ranges_${rel.name}->push_back(${e_name}_encoding.num_distinct);""")        
    } 
  }

  private def emitEncodingForEquivalenceClasses(s : CodeStringBuilder, klass : EquivalenceClasses, relsAttrs : Relations) = {
    val (encodingMap,attrMap,encodingNames) = klass
    val groupedTypes = encodingMap.groupBy(e => (e._2._1,e._2._3) ).map{e =>
      //1st group By (encodingID,encodingType) then create new map (encodingID -> (encoding Type, (ordered relations))) 
      (e._1._1,(e._1._2,e._2.keys.toList.sortBy(a => e._2(a)._2)))
    }
    encodingNames.foreach{n =>
      emitEncodingInitForAttr(s, n._2, groupedTypes(n._1)._1)
    }
    groupedTypes.keys.foreach{ gt =>
      groupedTypes(gt)._2.foreach{ ri =>
        emitAttrEncoding(s, encodingNames(gt), ri._1, ri._2)
      }
    }
    encodingNames.foreach{n =>
      emitBuildEncodingForAttr(s, n._2, groupedTypes(n._1)._1)
    }
    // emit code to specify the levels of the tries  
    relsAttrs.foreach((rel : Relation) => {
      emitEncodingRelations(s,rel,encodingMap,encodingNames)
    })
  }
  
  def emitTrieBuilding(s: CodeStringBuilder, relsAttrs : Relations) = {
    // emit code to construct each of the tries
    relsAttrs.foreach((relAttrs : Relation) => 
      s.println(s"""Trie<${layout}> *T${relAttrs.name} = Trie<${layout}>::build(E${relAttrs.name}, ranges_${relAttrs.name}, [&](size_t index){return true;});""") 
    )
  }

  def emitAndDetectSelections(s:CodeStringBuilder, attribute_ordering:List[String]) : List[List[ASTCriterion]] = {
    attribute_ordering.map((attr: String) => 
      selectCriteria.filter{ sc =>
        (sc.attr1,sc.attr2) match {
          case (a:ASTScalar,b:ASTStringLiteral) => 
            if(a.identifierName == attr){
              s.print("uint32_t " + attr + "_selection = " + attr + "_encoding.value_to_key.at(") 
              b.code(s)
              s.println(");")
              true
            } else 
              false
          case _ => false
        }
      }
    )
  }

  def emitNPRR(s:CodeStringBuilder, current:GHDNode, attribute_ordering:List[String], attrSelections:List[List[ASTCriterion]], aggregate:Boolean, equivalenceClasses:EquivalenceClasses) : Unit = {
    println("Bag ordering")
    attribute_ordering.foreach{
      println
    }

    //Figure out if an attribute is shared amongst relations...ie if we have to do work
    //Ask Kevin how you would write this functionally (seems like a common pattern)
    var work = 0
    current.rels.foreach{r1 =>
      current.rels.foreach{r2 =>
        if(r1.name != r2.name)
          work += r1.attrs.intersect(r2.attrs).size
      }
    }

    val name = current.rels.map(r => r.name).mkString("") + "_" + attribute_ordering.mkString("")
    //println("WORK: " + work)
    //if(work != 0){
      println("Running NPRR")
      current.rels.foreach{r => println(r.name)}
      s.println("par::reducer<size_t> output_cardinality(0,[](size_t a, size_t b){")
      s.println("return a + b;")
      s.println("});")
      s.println("//////////NPRR")
      s.println(s"""TrieBlock<${layout}> *${name}_block;""")
      s.println("{")
      val firstBlockOfTrie = current.rels.map(( rel: Relation) => ("T" + rel.name + "->head", rel.attrs))
      emitNPRR(s, true, attribute_ordering, firstBlockOfTrie, attrSelections, aggregate,equivalenceClasses,"","",name)
      s.println("}")
      s.println("size_t result = output_cardinality.evaluate(0);")
      s.println("std::cout << result << std::endl;")
    //} 
    /*
    else{
      s.println(s"""TrieBlock<${layout}> ${name} = """)
      println("NAME: " + name)
    }
    */
  }

  override def code(s: CodeStringBuilder): Unit = {
    /**
     * We get a distinct list of them so we can look them up and have a pointer to them
     */
    val relations = rels.map((rel : ASTRelation) => new Relation(rel.attrs, rel.identifierName))
    val distinctRelations = relations.groupBy(_.name).map(_._2.head).toList.sortBy(a => a.name) //distinct
    emitRelationLookupAndCast(s, distinctRelations.map(_.name))

    /**
     * Now emit the encodings of each of the attrs
     */
    val equivalenceClasses = buildEncodingEquivalenceClasses(relations)
    emitEncodingForEquivalenceClasses(s, equivalenceClasses, distinctRelations)

    //////////////////////////////////////////////////////////////////////
    //start solver stuff
    val solver = GHDSolver
    //get minimum GHD's
    val myghd = solver.getGHD(relations)
    val attribute_ordering = solver.getAttributeOrdering(myghd)
    //////////////////////////////////////////////////////////////////////

    /**
     * emit the trie building
     */
    emitTrieBuilding(s, distinctRelations)

    /**
     * Emit the buffers that we will do intersections for each attr in
     */
    var aggregate = false;
    //FIXME
    s.println(s"""allocator::memory<uint8_t> output_buffer(100000000);""")

    //Prepare the attributes that will need to be selected on the fly
    val attrSelections = emitAndDetectSelections(s,attribute_ordering)

    solver.bottom_up(mutable.LinkedHashSet[GHDNode](myghd), myghd, emitNPRR, s, attribute_ordering, attrSelections, aggregate, equivalenceClasses)
  }

  override def optimize: Unit = ???
}

case class ASTStringLiteral(str : String) extends ASTExpression {
  override def code(s: CodeStringBuilder): Unit = {
    s.print("\"" + str + "\"")
  }
  override def optimize: Unit = ???
}

abstract trait ASTCriterion extends ASTExpression{
  val attr1:ASTExpression
  val attr2:ASTExpression
}
case class ASTEq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = {
    attr1.code(s)
    s.print("==")
    attr2.code(s) 
  }
  override def optimize: Unit = ???
}
case class ASTLeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???
  override def optimize: Unit = ???
}
case class ASTGeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???

  override def optimize: Unit = ???
}
case class ASTLess(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???

  override def optimize: Unit = ???
}
case class ASTGreater(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???

  override def optimize: Unit = ???
}
case class ASTNeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???

  override def optimize: Unit = ???
}

abstract trait ASTIdentifier extends ASTExpression
case class ASTRelation(identifierName : String, attrs : List[String]) extends ASTIdentifier {
  override def code(s: CodeStringBuilder): Unit = ???
  override def optimize: Unit = ???
} // attribute name to option with type, or no type if it can be inferred
case class ASTScalar(identifierName : String) extends ASTIdentifier {
  override def code(s: CodeStringBuilder): Unit = {
    s.print(identifierName)
  }
  override def optimize: Unit = ???
}
