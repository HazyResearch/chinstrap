package DunceCap

import scala.collection.mutable

/**
 * All code generation should start from this object:
 */
object CodeGen {
  val layout = "hybrid"

  /**
   * This following method will get called in both compiler and repl mode
   */
  def emitHeaderAndCodeForAST(s: CodeStringBuilder, root: ASTNode) = {
    s.println("#include \"emptyheaded.hpp\"")
    s.println(s"""extern \"C\" void run(std::unordered_map<std::string, void*>& relations, std::unordered_map<std::string, Trie<${layout}>*> tries, std::unordered_map<std::string, std::vector<void*>*> encodings) {""")
    root.code(s)
    s.println("}")
  }

  /**
    * emitMainMethod will get called only in compiler mode
    */
  def emitMainMethod(s : CodeStringBuilder): Unit = {
    s.println("int main() {")
    s.println("std::unordered_map<std::string, void*> relations;")
    s.println(s"std::unordered_map<std::string, Trie<${layout}>*> tries;")
    s.println(s"std::unordered_map<std::string, std::vector<void*>*> encodings;")
    s.println("run(relations,tries,encodings);")
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
  val layout = CodeGen.layout
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
    println("ASSIGN")
    //FIXME: this is a hack
    (identifier,expression) match {
      case (a:ASTRelation,b:ASTJoinAndSelect) => {
        b.lhs = a
      }
    }
    expression.code(s)
  }

  override def optimize: Unit = ???
}
case class ASTPrintStatement(r : ASTRelation) extends ASTNode with ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    s.println(s"""Trie<${layout}> *T${r.identifierName} = tries["${r.identifierName}"] ;""")
    s.println(s"""std::vector<void*> *encodings_${r.identifierName} = encodings["${r.identifierName}"] ;""")

    val name = r.getName()
    val types = Environment.getTypes(r.identifierName)
    (0 until r.attrs.size).foreach{i =>
      val name_string = s"T${r.identifierName}->head" + (0 until i).map{i2 =>
        val a = r.attrs(i2)
        s"->get_block(${a}_d)"
      }.mkString("")
      s.println(s"""if(${name_string}){""")
      s.println(s"""${name_string}->set.foreach_index([&](uint32_t ${r.attrs(i)}_i, uint32_t ${r.attrs(i)}_d){""")
      s.println(s"""(void) ${r.attrs(i)}_i;""")
      if(i == (r.attrs.size-1)){
        s.print("""std::cout """)
        (0 until r.attrs.size).foreach{i2 =>
          val a = r.attrs(i2)
          s.print(s""" << ((Encoding<${types(i2)}>*)encodings_${r.identifierName}->at(${i2}))->key_to_value[${a}_d]""")
          if(i2 != r.attrs.size-1)
            s.print("""<< "\t" """)
        }
        s.println(""" << std::endl; """)
      }
    }
    (0 until (r.attrs.size)).foreach{ i =>
      s.println("""});};""")
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
  //FIXME: should re-factor how this is put in 
  var lhs:ASTRelation = ASTRelation("",List())
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
      s.print(";}")
    }
  }

  def emitSetSelection(s: CodeStringBuilder, attr:String, sel:List[ASTCriterion], tid:String, layout:String): Unit = {
    //FIXME: Implement a filter operation
    if(sel.size != 0){
      s.println(s"""uint8_t *sd_${attr} = output_buffer.get_next(${tid},${attr}_block->set.cardinality*sizeof(uint32_t)); //initialize the memory""")
      s.println(s"""uint32_t *ob_${attr} = (uint32_t*) output_buffer.get_next(${tid},${attr}_block->set.cardinality*sizeof(uint32_t));""")
      s.println(s""" size_t ob_i_${attr} = 0;""")
      s.println(s"""${attr}.foreach([&](uint32_t ${attr}_data){""")
      s.print(s"""if(""")
      emitSelectionCondition(s,sel.head)
      sel.tail.foreach{ i =>
        s.print(" && ")
        emitSelectionCondition(s,i)
      }
      s.println(")")
      s.println(s""" ob_${attr}[ob_i_${attr}++] = ${attr}_data;""")
      s.println("});")
      s.println(s"""${attr} = Set<${layout}>::from_array(sd_${attr},ob_${attr},ob_i_${attr});""")
      s.println(s"""output_buffer.roll_back(${tid},${attr}_block->set.cardinality*sizeof(uint32_t));""")
      s.println(s"""${attr}_block->set= &${attr};""")
    }
  }

  private def emitAttrIntersection(s: CodeStringBuilder, lastIntersection : Boolean, attr : String, sel: List[ASTCriterion], relsAttrs :  List[(String, List[String])], aggregate:Boolean, equivalenceClasses:EquivalenceClasses, prev_a:String, prev_t:String, name:String, yanna:List[(String, String)]) : List[(String, List[String])]= {
    val (encodingMap,attrMap,encodingNames,attributeToType) = equivalenceClasses
    val encodingName = encodingNames(attrMap(attr))

    val relAttrsA = relsAttrs.filter(( rel : (String, List[String])) => rel._2.contains(attr)).unzip._1.distinct
    println("HER YE SIZE: " + relAttrsA.size)
    val passedUp = yanna.filter{a =>
      a._1 == attr
    }.distinct.map(_._2)

    val relsAttrsWithAttr= relAttrsA ++ passedUp
    assert(!relsAttrsWithAttr.isEmpty)

    var tid = "tid"
    if(prev_a == "")
      tid = "0"

      println("SIZE 2: " + relsAttrsWithAttr.size)

    if (relsAttrsWithAttr.size == 1) {
      // no need to emit an intersection
      s.println( s"""Set<${layout}> ${attr} = ${relsAttrsWithAttr.head}->set;""")
      if(!aggregate){
        if(prev_a == ""){
          s.println(s"""${name}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout}>))) TrieBlock<${layout}>(${relsAttrsWithAttr.head});""")
          emitSetSelection(s,attr,sel,tid,layout)
          //s.println(s"""${name}_block->init_pointers(${tid},&output_buffer,${encodingName}_encoding.num_distinct);""")
          if(prev_a != ""){
            s.println(s"""if(${attr}.cardinality != 0)""")
            s.println(s"""${prev_t}_block->set_block(${prev_a}_i,${prev_a}_d,${attr}_block);""")      
          }
        }
        else{
          s.println(s"""TrieBlock<${layout}> *${attr}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout}>))) TrieBlock<${layout}>(${relsAttrsWithAttr.head});""")
          emitSetSelection(s,attr,sel,tid,layout)    
          s.println(s"""${attr}_block->init_pointers(${tid},&output_buffer,${attr}.cardinality,${encodingName}_encoding->num_distinct,${attr}.type == type::UINTEGER);""")      
          if(prev_a != ""){
            s.println(s"""if(${attr}.cardinality != 0)""")
            s.println(s"""${prev_t}_block->set_block(${prev_a}_i,${prev_a}_d,${attr}_block);""")  
          }
        }
      }
     } else {
      if(!aggregate){
        if(prev_a == "")
          s.println(s"""${name}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout}>))) TrieBlock<${layout}>();""")
        else
          s.println(s"""TrieBlock<${layout}> *${attr}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout}>))) TrieBlock<${layout}>();""")
      }
      s.println(s"""const size_t alloc_size = sizeof(uint64_t)*${encodingName}_encoding->num_distinct*2;""")
      s.println(s"""Set<${layout}> ${attr}(output_buffer.get_next(${tid},alloc_size)); //initialize the memory""")
      
      //have buffers you switch between for intersecting the rest (FIXME can leak memory)
      
      //intersection
      val restOfRelsAttrsWithAttr = relsAttrsWithAttr.tail.tail 
      //even number of intersections means tmp should be second, odd means temp should be first
      //even number of intersections corresponds to an odd relsAttrisWithAttr size
      //second condition tells us if even need a temp buffer or not
      var tmp = ((relsAttrsWithAttr.size % 2) != 0) && (restOfRelsAttrsWithAttr.size != 0)
      if(restOfRelsAttrsWithAttr.size != 0)
        s.println(s"""Set<${layout}> ${attr}_tmp(tmp_buffer.get_next(${tid},alloc_size)); //initialize the memory""")
      var a_name = if(tmp) attr + "_tmp" else attr
      s.print(s"""${a_name} = *ops::set_intersect(&${a_name},(const Set<${layout}> *)&${relsAttrsWithAttr.head}->set,(const Set<${layout}> *)&${relsAttrsWithAttr.tail.head}->set""")
      emitIntersectionSelection(s,attr,sel)
      s.println(");")

      restOfRelsAttrsWithAttr.foreach{(rel : String) => 
        tmp = !tmp
        if(tmp){
          s.print(s"""${attr}_tmp = *ops::set_intersect(&${attr}_tmp,(const Set<${layout}> *)&${attr},(const Set<${layout}> *)&${rel}->set);""")
        }
        else{
          s.print(s"""${attr} = *ops::set_intersect(&${attr},(const Set<${layout}> *)&${attr}_tmp,(const Set<${layout}> *)&${rel}->set);""")
        }
      }
      if(restOfRelsAttrsWithAttr.size != 0){
        s.println(s"""tmp_buffer.roll_back(${tid},alloc_size);""")
      }

      s.println(s"""output_buffer.roll_back(${tid},alloc_size - ${attr}.number_of_bytes);""")
      if(!aggregate){
        if(prev_a != ""){
          s.println(s"""${attr}_block->set= &${attr};""")
          s.println(s"""${attr}_block->init_pointers(${tid},&output_buffer,${attr}.cardinality,${encodingName}_encoding->num_distinct,${attr}.type == type::UINTEGER);""")
          s.println(s"""if(${attr}.cardinality != 0)""")
          s.println(s"""${prev_t}_block->set_block(${prev_a}_i,${prev_a}_d,${attr}_block);""")      
        }
        else{
          s.println(s"""${name}_block->set= &${attr};""")
          s.println(s"""${name}_block->init_pointers(${tid},&output_buffer,${attr}.cardinality,${encodingName}_encoding->num_distinct,${attr}.type == type::UINTEGER);""")
        }
      }
    }

    if (lastIntersection) {
      s.println(s"""const size_t count = ${attr}.cardinality;""")
      s.println(s"""${name}_cardinality.update(${tid},count);""")
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

  private def emitAttrLoopOverResult(s: CodeStringBuilder, outermostLoop : Boolean, attrs : List[String], relsAttrs : List[(String, List[String])], attrSelections:List[List[ASTCriterion]], aggregate:Boolean, equivalenceClasses:EquivalenceClasses, prev_a:String, prev_t:String, name:String, yanna:List[(String, String)]) = {
    // this should include the walking down the trie, so that when you recursively call emitNPRR, you do so with different rel names
    if (outermostLoop) {
      if(aggregate)
        s.println(s"""${attrs.head}.par_foreach([&](size_t tid, uint32_t ${attrs.head}_d){""")
      else 
        s.println(s"""${attrs.head}.par_foreach_index([&](size_t tid, uint32_t ${attrs.head}_i, uint32_t ${attrs.head}_d){""")
      emitNPRR(s, false, attrs.tail, relsAttrs,attrSelections.tail,aggregate,equivalenceClasses,prev_a,prev_t,name,yanna)
      s.println("});")
    } else {
      if(aggregate)
        s.println(s"""${attrs.head}.foreach([&](uint32_t ${attrs.head}_d) {""")
      else
        s.println(s"""${attrs.head}.foreach_index([&](uint32_t ${attrs.head}_i, uint32_t ${attrs.head}_d) {""")
      emitNPRR(s, false, attrs.tail, relsAttrs,attrSelections.tail,aggregate,equivalenceClasses,prev_a,prev_t,name,yanna)
      s.println("});")
    }
  }

  private def emitNPRR(s: CodeStringBuilder, initialCall : Boolean, attrs : List[String], relsAttrs : List[(String, List[String])], attrSelections:List[List[ASTCriterion]], aggregate:Boolean, equivalenceClasses:EquivalenceClasses, prev_a:String, prev_t:String, name:String, yanna:List[(String, String)]) : Unit = {
    if (attrs.isEmpty) return

    val currAttr = attrs.head
    if (attrs.tail.isEmpty) {
      emitAttrIntersection(s, true, currAttr, attrSelections.head, relsAttrs, aggregate, equivalenceClasses,prev_a,prev_t,name,yanna)
    } else {
      val updatedRelsAttrs = emitAttrIntersection(s, false, currAttr, attrSelections.head, relsAttrs, aggregate, equivalenceClasses,prev_a,prev_t,name,yanna)
      val pt = if(prev_a == "") name else currAttr
      emitAttrLoopOverResult(s, initialCall, attrs, updatedRelsAttrs, attrSelections, aggregate,equivalenceClasses,currAttr,pt,name,yanna)
    }
  }

  private def emitAttrEncoding(s : CodeStringBuilder, rewrittenAttrName : String, relName: String, attrIndex: Int): Unit = {
    s.println( s"""${rewrittenAttrName}_attributes->push_back(${relName}->get<${attrIndex}>()); """)
  }

  private def emitEncodingInitForAttr(s: CodeStringBuilder, attr : String, attrType: String) = {
    s.println(s"""std::vector<Column<${attrType}>> *${attr}_attributes = new std::vector<Column<${attrType}>>();""")
  }

  private def emitBuildEncodingForAttr(s: CodeStringBuilder,  attr : String, attrType: String) = {
    s.println(s"""Encoding<${attrType}> *${attr}_encoding = new Encoding<${attrType}>(${attr}_attributes);""")
  }

  private def emitEncodingRelations(s:CodeStringBuilder, rel:Relation, encodingMap:EM, encodingNames:Map[Int,String]) = {
    s.println(s"""std::vector<Column<uint32_t>> *E${rel.name} = new std::vector<Column<uint32_t>>();""")
    s.println(s"""std::vector<void *> *encodings_${rel.name} = new std::vector<void*>();""")

    (0 until rel.attrs.size).toList.foreach{ i =>
      val eID = encodingMap((rel.name,i))
      val e_name = encodingNames(eID._1)
      val e_index = eID._2
      s.println(s"""E${rel.name}->push_back(${e_name}_encoding->encoded.at(${e_index}));""")
      s.println(s"""encodings_${rel.name}->push_back((void*)${e_name}_encoding);""")
    } 
  }

  private def emitEncodingForEquivalenceClasses(s : CodeStringBuilder, klass : EquivalenceClasses, relsAttrs : Relations) = {
    val (encodingMap,attrMap,encodingNames,attributeToType) = klass
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
    relsAttrs.foreach{(relAttrs : Relation) => 
      s.println(s"""Trie<${layout}> *T${relAttrs.name} = Trie<${layout}>::build(E${relAttrs.name}, [&](size_t index){(void) index; return true;});""") 
      s.println(s"""tries["${relAttrs.name}"] = T${relAttrs.name};""")
      s.println(s"""encodings["${relAttrs.name}"] = encodings_${relAttrs.name};""") 
    }
  }

  def emitAndDetectSelections(s:CodeStringBuilder, attribute_ordering:List[String], eq:EquivalenceClasses) : List[List[ASTCriterion]] = {
    val (encodingMap,attrMap,encodingNames,attributeToType) = eq
    attribute_ordering.map((attr: String) => 
      selectCriteria.filter{ sc =>
        (sc.attr1,sc.attr2) match {
          case (a:ASTScalar,b:ASTStringLiteral) => 
            if(a.identifierName == attr){
              s.print("uint32_t " + attr + "_selection = " + encodingNames(attrMap(attr)) + "_encoding->value_to_key.at(") 
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

  def emitResultTrie(s:CodeStringBuilder, identifierName:String, blockName:String, attribute_ordering:List[String], eq:EquivalenceClasses) : Unit = {
    val (encodingMap,attrMap,encodingNames,attributeToType) = eq
    s.println(s"""Trie<${layout}> *T${identifierName} = new Trie<${layout}>(${blockName}_block);""")
    s.println(s"""tries["${identifierName}"] = T${identifierName};""")
    s.println(s"""std::vector<void*> *encodings_${identifierName} = new std::vector<void*>();""")
    attribute_ordering.foreach{a =>
      val ename = encodingNames(attrMap(a))
      s.println(s"""encodings_${identifierName}->push_back(${ename}_encoding);""")
    }
    s.println(s"""encodings["${identifierName}"] =  encodings_${identifierName};""")
    val newTypes = attribute_ordering.map(attributeToType(_))
    Environment.addRelationBinding(lhs.identifierName,newTypes)
  }

  def emitNPRR(s:CodeStringBuilder, current:GHDNode, attribute_ordering:List[String], attrSelections:List[List[ASTCriterion]], aggregate:Boolean, equivalenceClasses:EquivalenceClasses) : Unit = {
    println("Bag ordering")
    current.attribute_ordering = attribute_ordering
    attribute_ordering.foreach{
      println
    }

    //Figure out if an attribute is shared amongst relations...ie if we have to do work
    //Ask Kevin how you would write this functionally (seems like a common pattern)
    var work_to_do = true
    if(current.rels.size == 1){
      val r = current.rels.head
      if(r.attrs == attribute_ordering){
        work_to_do = false
      }
    }
    work_to_do |= (attrSelections.map(e => e.size).reduce((a,b) => a + b) != 0)

    val name = current.getName(attribute_ordering)
    current.name = name

    val yanna = attribute_ordering.flatMap{ a =>
      current.children.flatMap{ c =>
        (0 until c.attribute_ordering.size).filter(c.attribute_ordering(_) == a).map{ i =>
          val result = c.name + "_block"
          (a,result + (0 until i).map{ s =>
            "->get_block(" + c.attribute_ordering(s) + "_d)" 
          }.mkString(""))
        }
      }
    }
    work_to_do |= yanna.size != 0

    //println("WORK: " + work)
    //if(work != 0){
    println("Running NPRR")
    s.println("//////////NPRR")
    current.rels.foreach{r => println(r.name)}
    if(work_to_do){
      s.println(s"""par::reducer<size_t> ${name}_cardinality(0,[](size_t a, size_t b){""")
      s.println("return a + b;")
      s.println("});")
      s.println(s"""TrieBlock<${layout}> *${name}_block;""")
      s.println("{")
      val firstBlockOfTrie = current.rels.map(( rel: Relation) => ("T" + rel.name + "->head", rel.attrs))
      emitNPRR(s, true, attribute_ordering, firstBlockOfTrie, attrSelections, aggregate,equivalenceClasses,"","",name,yanna)
      s.println("}")
      s.println(s"std::cout << ${name}_cardinality.evaluate(0) << std::endl;")
    } else{
      val r_name = current.rels.head.name
      s.println(s"""TrieBlock<${layout}> *${name}_block = (TrieBlock<${layout}>*) T${r_name};""")
    }
  }

  def emitTopDown(s:CodeStringBuilder, accessor:Map[String,String], checks:Map[String,mutable.Set[String]], attribute_ordering:List[String], eq:EquivalenceClasses) = {
    val lhs_name = lhs.getName()
    val (encodingMap,attrMap,encodingNames,attributeToType) = eq
    s.println("///////////////////TOP DOWN")
    //FIXME ADD A REAL OUTPUT TRIE
    s.println(s"""par::reducer<size_t> ${lhs_name}_cardinality(0,[](size_t a, size_t b){""")
    s.println("return a + b;")
    s.println("});")
    s.println(s"""TrieBlock<${layout}> *${lhs_name}_block;""")

    s.println("{")
    val visited_attributes = mutable.Set[String]()
    (0 until attribute_ordering.size).foreach{ i =>
      val a = attribute_ordering(i)
      val a_name = if(i != 0) a else lhs_name
      val access = accessor(a)
      val prev = if(i == 0) "" else attribute_ordering(i-1)
      val prev_name = if(i == 0) "" else if(i == 1) lhs_name else attribute_ordering(i-1)
      if(!visited_attributes.contains(a)){
        if(i != 0)
          s.println(s"""TrieBlock<${layout}> *${a}_block = ${access};""")
        else
          s.println(s"""${lhs_name}_block = ${access};""")
        visited_attributes += a
      }
      if(checks.contains(prev)){
        checks(prev).foreach{ check =>
          if(!visited_attributes.contains(check)){
            val next = accessor(check)
            s.println(s"""TrieBlock<${layout}> *${check}_block = ${next};""")
            visited_attributes += check
          }
        }
      }
        s.print("if(" + a_name + "_block")
        if(i != (attribute_ordering.size-1) && checks.contains(prev)){
          checks(prev).foreach{check =>
            if(check != a){
              s.print(" && ")
              s.print(s"${check}_block")
            }
          }
        }
        s.println("){")

      if(i != 0){
        s.println(s"""${a_name}_block = new(output_buffer.get_next(tid, sizeof(TrieBlock<${layout}>))) TrieBlock<${layout}>(${a_name}_block);""")
        s.println(s"""if(${a}_block->set.cardinality != 0)""")
        s.println(s"""${prev_name}_block->set_block(${prev}_i,${prev}_d,${a}_block);""")
        if(i != (attribute_ordering.size-1)){
          val ename = encodingNames(attrMap(a))
          s.println(s"""${a_name}_block->init_pointers(tid, &output_buffer,${a_name}_block->set.cardinality,${ename}_encoding->num_distinct,${a}_block->set.type == type::UINTEGER);""")
          s.println(s"""${a_name}_block->set.foreach_index([&](uint32_t ${a}_i, uint32_t ${a}_d) {""")
        } else{
          //FIXME ADD A REAL TRIE OUTPUT NAME
          s.println(s"""const size_t count = ${a}_block->set.cardinality;""")
          s.println(s"""${lhs_name}_cardinality.update(tid,count);""")
        }
      } else 
        s.println(s"""${a_name}_block->set.par_foreach_index([&](size_t tid, uint32_t ${a}_i, uint32_t ${a}_d) {""")
      
    }
    (0 until attribute_ordering.size-1).foreach{ i =>
      //closes out if
      if(i == 0)
        s.println("}")
      //closes out if and foreach
      s.println("""});}""")
    }
    emitResultTrie(s, lhs.identifierName, lhs_name, attribute_ordering, eq)
    s.println("}")
    s.println(s"std::cout << ${lhs_name}_cardinality.evaluate(0) << std::endl;")
  }

  override def code(s: CodeStringBuilder): Unit = {
    //////////////////////////////////////////////////////////////////////
    //start solver stuff
    var relations = rels.map((rel : ASTRelation) => new Relation(rel.attrs, rel.identifierName))
    val solver = GHDSolver
    //get minimum GHD's
    val myghd = solver.getGHD(relations)
    val attribute_ordering = solver.getAttributeOrdering(myghd)
    //////////////////////////////////////////////////////////////////////

    /**
     * We get a distinct list of them so we can look them up and have a pointer to them
     */
    relations = rels.map((rel : ASTRelation) => new Relation(rel.attrs.sortBy(e => attribute_ordering.indexOf(e)), rel.identifierName))
    val distinctRelations = relations.groupBy(_.name).map(_._2.head).toList.sortBy(a => a.name) //distinct
    emitRelationLookupAndCast(s, distinctRelations.map(_.name))

    /**
     * Now emit the encodings of each of the attrs
     */
    val equivalenceClasses = buildEncodingEquivalenceClasses(relations)
    emitEncodingForEquivalenceClasses(s, equivalenceClasses, distinctRelations)

    /**
     * emit the trie building
     */
    emitTrieBuilding(s, distinctRelations)

    /**
     * Emit the buffers that we will do intersections for each attr in
     */
    var aggregate = false;
    val (e_to_index2,attributeToEncoding,encodingIDToName,attributeToType) = equivalenceClasses
    val size_string = encodingIDToName.head._2 + "_encoding->num_distinct" + encodingIDToName.tail.map{ e =>
      s"""+${e._2}_encoding->num_distinct"""
    }.mkString("")
    s.println(s"""allocator::memory<uint8_t> output_buffer(${myghd.num_bags}*${attribute_ordering.size}*2*sizeof(TrieBlock<${layout}>)*(${size_string}));""")
    s.println(s"""allocator::memory<uint8_t> tmp_buffer(${myghd.num_bags}*${attribute_ordering.size}*2*sizeof(TrieBlock<${layout}>)*(${size_string}));""")

    //Prepare the attributes that will need to be selected on the fly
    val attrSelections = emitAndDetectSelections(s,attribute_ordering,equivalenceClasses)

    s.println(s"""auto join_timer = debug::start_clock();""")
    solver.bottom_up(mutable.LinkedHashSet[GHDNode](myghd), myghd, emitNPRR, s, attribute_ordering, attrSelections, aggregate, equivalenceClasses)
    if(myghd.children.size != 0){
      val (accessor,checks) = solver.top_down(mutable.LinkedHashSet[GHDNode](myghd), mutable.LinkedHashSet(myghd))
      emitTopDown(s,accessor,checks,attribute_ordering,equivalenceClasses) 
    } else{
      emitResultTrie(s, lhs.identifierName, myghd.name, myghd.attribute_ordering, equivalenceClasses)
    }
    s.println(s"""debug::stop_clock("JOIN",join_timer);""")
    s.println(s"""tmp_buffer.free();""")


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
  def getName() : String = {
    identifierName + "_" + attrs.mkString("")
  }
} // attribute name to option with type, or no type if it can be inferred
case class ASTScalar(identifierName : String) extends ASTIdentifier {
  override def code(s: CodeStringBuilder): Unit = {
    s.print(identifierName)
  }
  override def optimize: Unit = ???
}
