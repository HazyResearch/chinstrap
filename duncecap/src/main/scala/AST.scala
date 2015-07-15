package DunceCap

import scala.collection.mutable
/**
 * All code generation should start from this object:
 */
object CodeGen {
  var layout = "hybrid"

  /**
   * This following method will get called in both compiler and repl mode
   */
  def emitHeaderAndCodeForAST(s: CodeStringBuilder, root: ASTNode) = {
    s.println("#define GENERATED")
    s.println("#include \"main.hpp\"")
    s.println(s"""extern \"C\" long run(std::unordered_map<std::string, void*>& relations, std::unordered_map<std::string, Trie<${layout}>*> tries, std::unordered_map<std::string, std::vector<void*>*> encodings) {""")
    s.println(s"""long query_result = -1;""")
    root.code(s)
    s.println(s"""return query_result;""")
    s.println("}")
  }

  /**
    * emitMainMethod will get called only in compiler mode
    */
  def emitMainMethod(s : CodeStringBuilder): Unit = {
    s.println("#ifndef GOOGLE_TEST")
    s.println("int main() {")
    s.println("std::unordered_map<std::string, void*> relations;")
    s.println(s"std::unordered_map<std::string, Trie<${layout}>*> tries;")
    s.println(s"std::unordered_map<std::string, std::vector<void*>*> encodings;")
    s.println("run(relations,tries,encodings);")
    s.println("}")
    s.println("#endif")
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
case class ASTSumStatement(r : ASTRelation) extends ASTNode with ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    s.println(s"""par::reducer<size_t> ${r.identifierName}_cardinality(0,[](size_t a, size_t b){""")
    s.println("return a + b;")
    s.println("});")
    s.println(s"""Trie<${layout}> *T${r.identifierName} = tries["${r.identifierName}"] ;""")

    val name = r.getName()
    val types = Environment.getTypes(r.identifierName)
    (0 until r.attrs.size-1).foreach{i =>
      val name_string = s"T${r.identifierName}->head" + (0 until i).map{i2 =>
        val a = r.attrs(i2)
        s"->get_block(${a}_i,${a}_d)"
      }.mkString("")
      s.println(s"""if(${name_string}){""")
      if(i == 0)
        s.println(s"""${name_string}->set.par_foreach_index([&](size_t tid, uint32_t ${r.attrs(i)}_i, uint32_t ${r.attrs(i)}_d){""")
      else
        s.println(s"""${name_string}->set.foreach_index([&](uint32_t ${r.attrs(i)}_i, uint32_t ${r.attrs(i)}_d){""")
      s.println(s"""(void) ${r.attrs(i)}_i;""")
      if(i == (r.attrs.size-2)){
        val name_string = s"T${r.identifierName}->head" + (0 until (r.attrs.size-1)).map{i2 =>
        val a = r.attrs(i2)
        s"->get_block(${a}_i,${a}_d)"
      }.mkString("")
        s.println(s"""if(${name_string}){""")
        s.println(s"""${r.identifierName}_cardinality.update(tid,${name_string}->set.cardinality);""")
        s.println(s"""}""")
      }
    }
    (0 until (r.attrs.size-1)).foreach{ i =>
      s.println("""});}""")
    }
    s.println(s"""query_result = ${r.identifierName}_cardinality.evaluate(0);""")
    s.println(s"""std::cout << ${r.identifierName}_cardinality.evaluate(0) << std::endl;""")
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
        s"->get_block(${a}_i,${a}_d)"
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
      s.println("""});}""")
    }
  }

  override def optimize: Unit = ???
}

abstract trait ASTExpression extends ASTNode
case class ASTCount(expression : ASTExpression) extends ASTExpression {
  override def code(s: CodeStringBuilder): Unit = ???

  override def optimize: Unit = ???
}

case class ASTJoinAndSelect(rels : List[ASTRelation], selectCriteria : List[ASTCriterion], aggregate: Boolean) extends ASTExpression {
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

  def emitSetSelection(s: CodeStringBuilder, attr:String, 
    name:String, sel:List[ASTCriterion], tid:String, 
    layout:String, aggregate:Boolean, setBlock:Boolean): Unit = {
    //FIXME: Implement a filter operation
    if(sel.size != 0){
      if(!aggregate){
        s.println(s"""uint8_t *sd_${attr} = output_buffer.get_next(${tid},${attr}.cardinality*sizeof(uint32_t)); //initialize the memory""")
        s.println(s"""uint32_t *ob_${attr} = (uint32_t*) output_buffer.get_next(${tid},${attr}.cardinality*sizeof(uint32_t));""")
      }
      s.println(s""" size_t ob_i_${attr} = 0;""")
      s.println(s"""${attr}.foreach([&](uint32_t ${attr}_data){""")
      s.print(s"""if(""")
      emitSelectionCondition(s,sel.head)
      sel.tail.foreach{ i =>
        s.print(" && ")
        emitSelectionCondition(s,i)
      }
      s.println(")")
      if(!aggregate)
        s.println(s""" ob_${attr}[ob_i_${attr}++] = ${attr}_data;""")
      else 
        s.println(s"""ob_i_${attr}++;""")
      s.println("});")
      if(!aggregate){
        s.println(s"""${attr} = Set<${layout}>::from_array(sd_${attr},ob_${attr},ob_i_${attr});""")
        s.println(s"""output_buffer.roll_back(${tid},${attr}.cardinality*sizeof(uint32_t));""")
        if(setBlock)
          s.println(s"""${name}_block->set= &${attr};""")
      } else{
        s.println(s"""${attr}.cardinality = ob_i_${attr};""")
      }
    }
  }
  private def emitInitAggValues(s:CodeStringBuilder,name:String,attr:String,encodingName:String,init_agg_values:Boolean,tid:String,force_dense_block:Boolean){
    if(init_agg_values){
      if(force_dense_block)
        s.println(s"""${name}_block->is_sparse = false;""")
      else
        s.println(s"""${name}_block->is_sparse = ${attr}.type == type::UINTEGER;""")
      s.println(s"""${name}_block->alloc_data(${tid},&output_buffer,${attr}.cardinality,${encodingName}_encoding->num_distinct);""")
    }
  }
  private def emitInitPointers(s:CodeStringBuilder,name:String,attr:String,encodingName:String,lastIntersection:Boolean,tid:String) = {
    if(!lastIntersection)
      s.println(s"""${name}_block->init_pointers(${tid},&output_buffer,${attr}.cardinality,${encodingName}_encoding->num_distinct,${attr}.type == type::UINTEGER);""")
  }

  private def emitAttrIntersection(s: CodeStringBuilder, lastIntersection : Boolean, attr : String, 
    sel: List[ASTCriterion], relsAttrs :  List[(String, List[String])], aggregate:Boolean, 
    equivalenceClasses:EquivalenceClasses, name:String, yanna:List[(String, String)], 
    processed:List[String], resultProcessed:List[String], resultAttrs:List[String], 
    init_agg_values:Boolean, lastBag:Boolean, new_head_data:Boolean) : (List[(String, List[String])],Boolean) = {
    val (encodingMap,attrMap,encodingNames,attributeToType) = equivalenceClasses
    val encodingName = encodingNames(attrMap(attr))

    val relAttrsA = relsAttrs.filter(( rel : (String, List[String])) => rel._2.contains(attr)).unzip._1.distinct
    val passedUp = yanna.filter{a =>
      a._1 == attr
    }.distinct.map(_._2)

    val relsAttrsWithAttr= relAttrsA ++ passedUp
    assert(!relsAttrsWithAttr.isEmpty)

    var tid = "tid"
    if(processed.size == 0)
      tid = "0"

    s.println(s"""Set<${layout}> ${attr};""")
    if(resultAttrs.contains(attr) && !aggregate){
      if(resultProcessed.size == 0){
        s.println(s"""${name}_block = NULL;""")
      } else{
        s.println(s"""TrieBlock<${layout},size_t> *${attr}_block = NULL;""")
      }
    }

    val alloc_buffer = relsAttrsWithAttr.size != 1 && aggregate
    //////////should be a method emit intersections
    if (alloc_buffer) {
      s.println(s"""const size_t alloc_size_${attr} = sizeof(uint64_t)*${encodingName}_encoding->num_distinct*2;""")
      s.println(s"""${attr}.data = output_buffer.get_next(${tid},alloc_size_${attr}); //initialize the memory""")
    }

    //can have NULL pointers in trie
    s.print(s"""if( ${relsAttrsWithAttr.head}""")
    relsAttrsWithAttr.tail.foreach{rawa =>
      s.print(s""" && ${rawa}""")
    }
    s.println(s"""){""")

    val force_dense_block = new_head_data && processed.size == 0
    if (relsAttrsWithAttr.size == 1) {
      // no need to emit an intersection
      s.println( s"""${attr} = ${relsAttrsWithAttr.head}->set;""")
      if(!aggregate){
        if(resultProcessed.size == 0){
          if(resultAttrs.contains(attr))
            s.println(s"""${name}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout},size_t>))) TrieBlock<${layout},size_t>(${relsAttrsWithAttr.head});""")
          emitSetSelection(s,attr,name,sel,tid,layout,!resultAttrs.contains(attr) && lastIntersection,resultAttrs.contains(attr))
          if(resultAttrs.contains(attr) && !aggregate && !init_agg_values)
            emitInitPointers(s,name,attr,encodingName,lastIntersection,tid)
          emitInitAggValues(s,name,attr,encodingName,init_agg_values,tid,force_dense_block)
        } else{
          if(resultAttrs.contains(attr))
            s.println(s"""${attr}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout},size_t>))) TrieBlock<${layout},size_t>(${relsAttrsWithAttr.head});""")
          emitSetSelection(s,attr,attr,sel,tid,layout,!resultAttrs.contains(attr) && lastIntersection,resultAttrs.contains(attr)) 
          if(resultAttrs.contains(attr) && !aggregate && !init_agg_values)
            emitInitPointers(s,attr,attr,encodingName,lastIntersection,tid)
          emitInitAggValues(s,attr,attr,encodingName,init_agg_values,tid,force_dense_block)
        }
      } else {
        emitSetSelection(s,attr,attr,sel,tid,layout,!resultAttrs.contains(attr) && lastIntersection,resultAttrs.contains(attr)) 
      }
     } else {
      if(resultAttrs.contains(attr) && !aggregate){
        if(resultProcessed.size == 0){
          s.println(s"""${name}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout},size_t>))) TrieBlock<${layout},size_t>();""")
        } else{
          s.println(s"""${attr}_block = new(output_buffer.get_next(${tid},sizeof(TrieBlock<${layout},size_t>))) TrieBlock<${layout},size_t>();""")
        }
      }

      //////////should be a method emit intersections          
      if(!aggregate){
        s.println(s"""const size_t alloc_size_${attr} = sizeof(uint64_t)*${encodingName}_encoding->num_distinct*2;""")
        s.println(s"""${attr}.data = output_buffer.get_next(${tid},alloc_size_${attr}); //initialize the memory""")
      }  

      //intersection
      val restOfRelsAttrsWithAttr = relsAttrsWithAttr.tail.tail 
      //even number of intersections means tmp should be second, odd means temp should be first
      //even number of intersections corresponds to an odd relsAttrisWithAttr size
      //second condition tells us if even need a temp buffer or not
      var tmp = ((relsAttrsWithAttr.size % 2) != 0) && (restOfRelsAttrsWithAttr.size != 0)
      if(restOfRelsAttrsWithAttr.size != 0)
        s.println(s"""Set<${layout}> ${attr}_tmp(tmp_buffer.get_next(${tid},alloc_size_${attr})); //initialize the memory""")
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
        s.println(s"""tmp_buffer.roll_back(${tid},alloc_size_${attr});""")
      }

      if(!aggregate)
        s.println(s"""output_buffer.roll_back(${tid},alloc_size_${attr} - ${attr}.number_of_bytes);""")

      
      if(resultAttrs.contains(attr) && !aggregate){
        if(processed.size != 0){
          s.println(s"""${attr}_block->set = &${attr};""")
          if(!init_agg_values)
            emitInitPointers(s,attr,attr,encodingName,lastIntersection,tid)
          emitInitAggValues(s,attr,attr,encodingName,init_agg_values,tid,force_dense_block)

        } else{
          s.println(s"""${name}_block->set= &${attr};""")
          if(!init_agg_values)
            emitInitPointers(s,name,attr,encodingName,lastIntersection,tid)
          emitInitAggValues(s,name,attr,encodingName,init_agg_values,tid,force_dense_block)
        }
      }
    }
    ////////////////////////////////////////// end intersection method

    if(lastIntersection && init_agg_values){
      if(processed.size != 0){
          s.println(s"""${attr}_block->init_data(1,${attr}.cardinality,${encodingName}_encoding->num_distinct);""")  
        } else{
          s.println(s"""${name}_block->init_data(1,${attr}.cardinality,${encodingName}_encoding->num_distinct);""")  
        }
    }

    s.println("}")

    if (lastIntersection) {
      if(aggregate){
          s.println(s"""${name}_cardinality.update(${tid},${attr}.cardinality);""")
          if(!init_agg_values && !lastBag){
            s.println(s"""aggregate_value += ${attr}.cardinality;""")
          }
          if(relsAttrsWithAttr.size != 1){
            s.println(s"""output_buffer.roll_back(${tid},alloc_size_${attr});""")
          }
          //will break code if it is not spitting this out for the outermost level of the trie...HACK
          if(new_head_data){
            s.println(s"""if(${attr}.cardinality != 0){""")
            s.println(s"""new_head_data[nhd.fetch_add(1)] = ${resultAttrs.head}_d;""")
            s.println(s"""}""")
          }
        } else {
          s.println(s"""if(${attr}.cardinality != 0){""")
          //update the count
          if(!resultAttrs.contains(attr)){
            s.println(s"""const size_t count = 1;""")
          }else{
            s.println(s"""const size_t count = ${attr}.cardinality;""")
          } 
          s.println(s"""${name}_cardinality.update(${tid},count);""")

          //will break code if it is not spitting this out for the outermost level of the trie...HACK
          if(new_head_data){
            s.println(s"""new_head_data[nhd.fetch_add(1)] = ${resultAttrs.head}_d;""")
          }

          if(resultAttrs.contains(attr)){
            resultProcessed.tail.filter(a => a != resultProcessed.head).foreach{ pa =>
              s.println(s"""${pa}_block_valid = true;""")
            }
            val blockName = if(resultProcessed.size == 1) name else resultProcessed.last
            s.println(s"""${blockName}_block->set_block(${resultProcessed.last}_i,${resultProcessed.last}_d,${attr}_block);""")
          } 
          else{
            resultProcessed.tail.foreach{ pa =>
              s.println(s"""${pa}_block_valid = true;""")
            }
          }
          if(resultAttrs.contains(attr)){
            s.println(s"""} else{""")
            val blockName = if(resultProcessed.size == 1) name else resultProcessed.last
            s.println(s"""${blockName}_block->set_block(${resultProcessed.last}_i,${resultProcessed.last}_d,NULL);""")
          }
          s.println("""}""")
        }
        (List[(String, List[String])](),alloc_buffer)
    } else {
      // update relsAttrs by peeling attr off the attribute lists, and adding ->map.at(attr_i) to the relation name
      (relsAttrs.map((rel : (String, List[String])) => {
        if (!rel._2.isEmpty && rel._2.head.contains(attr)) {
          (rel._1 + s"->get_block(${attr}_d)", rel._2.tail)
        } else {
          rel
        }
      }),alloc_buffer)
    }
  }

  private def emitAttrLoopOverResult(s: CodeStringBuilder, outermostLoop : Boolean, attrs : List[String], 
    relsAttrs : List[(String, List[String])], attrSelections:List[List[ASTCriterion]], aggregate:Boolean, 
    lastBag:Boolean, equivalenceClasses:EquivalenceClasses, name:String, yanna:List[(String, String)], 
    processed:List[String], resultProcessed:List[String], resultAttrs:List[String], 
    init_agg_values:Boolean, alloc_buffer:Boolean, new_data_for_head:Boolean) = {
    // this should include the walking down the trie, so that when you recursively call emitNPRR, you do so with different rel names
    val next_processed = if(resultAttrs.contains(attrs.head)) resultProcessed++List(attrs.head) else resultProcessed
    val agg_in = (aggregate && lastBag) || (aggregate && !resultAttrs.contains(attrs.head))

    if(resultAttrs.contains(attrs.head) && resultProcessed.size != 0 && !agg_in)
      s.println(s"""bool ${attrs.head}_block_valid = false;""")

    if (outermostLoop) {
      if(new_data_for_head){
        s.println(s"""uint32_t *new_head_data = (uint32_t*)tmp_buffer.get_next(0,${attrs.head}.cardinality*sizeof(uint32_t));""")
        s.println(s"""std::atomic<size_t> nhd(0);""")
      }

      if(agg_in)
        s.println(s"""${attrs.head}.par_foreach([&](size_t tid, uint32_t ${attrs.head}_d){""")
      else
        s.println(s"""${attrs.head}.par_foreach_index([&](size_t tid, uint32_t ${attrs.head}_i, uint32_t ${attrs.head}_d){""")
      
      if(init_agg_values){
          s.println("size_t aggregate_value = 0;")
      }

      emitNPRR(s, false, attrs.tail, relsAttrs,attrSelections.tail,
        aggregate,lastBag,equivalenceClasses,name,yanna,
        processed++List(attrs.head),next_processed,resultAttrs,new_data_for_head)
      
      if(init_agg_values){
        if(resultProcessed.size == 0)
          s.println(s"""${name}_block->set_data(${attrs.head}_i,${attrs.head}_d,aggregate_value);""")
        else
          s.println(s"""${attrs.head}_block->set_data(${attrs.head}_i,${attrs.head}_d,aggregate_value);""")
      }

      s.println("});")

      if(new_data_for_head){
        val (encodingMap,attrMap,encodingNames,attributeToType) = equivalenceClasses
        val encodingName = encodingNames(attrMap(attrs.head))
        s.println(s"""const size_t halloc_size = sizeof(uint64_t)*${encodingName}_encoding->num_distinct*2;""")
        s.println(s"""uint8_t *new_head_mem = output_buffer.get_next(${0},halloc_size);""")
        s.println(s"""tbb::parallel_sort(new_head_data,new_head_data+nhd.load());""");
        s.println(s"""${attrs.head} = Set<${layout}>::from_array(new_head_mem,new_head_data,nhd.load());""")
        s.println(s"""${name}_block->set = &${attrs.head};""")
        s.println(s"""output_buffer.roll_back(0,halloc_size-${attrs.head}.number_of_bytes);""")
      }
    } else {
      if(agg_in){
        s.println(s"""${attrs.head}.foreach([&](uint32_t ${attrs.head}_d) {""")
      } else{
        s.println(s"""${attrs.head}.foreach_index([&](uint32_t ${attrs.head}_i, uint32_t ${attrs.head}_d) {""")
      }

      if(init_agg_values){
          s.println("size_t aggregate_value = 0;")
      }

      emitNPRR(s, false, attrs.tail, relsAttrs, attrSelections.tail,
        aggregate,lastBag,equivalenceClasses,name,yanna,processed++List(attrs.head),
        next_processed,resultAttrs,new_data_for_head)
        
      if(init_agg_values){
        if(resultProcessed.size == 0)
          s.println(s"""${name}_block->set_data(${attrs.head}_i,${attrs.head}_d,aggregate_value);""")
        else
          s.println(s"""${attrs.head}_block->set_data(${attrs.head}_i,${attrs.head}_d,aggregate_value);""")
      }
      s.println("});")

      //when we have aggregates don't waste memory
      if(alloc_buffer){
        s.println(s"""output_buffer.roll_back(tid,alloc_size_${attrs.head});""")
      }
    }
    
    if(resultAttrs.contains(attrs.head) && resultProcessed.size != 0 && !agg_in){
      val blockName = if(resultProcessed.size == 1) name else resultProcessed.last
      s.println(s"""if(${attrs.head}_block_valid){""")
      s.println(s"""${blockName}_block->set_block(${resultProcessed.last}_i,${resultProcessed.last}_d,${attrs.head}_block);""")
      s.println(s"""} else{""")
      s.println(s"""${blockName}_block->set_block(${resultProcessed.last}_i,${resultProcessed.last}_d,NULL);""")
      s.println(s"""}""")
    }
  }

  private def emitNPRR(s: CodeStringBuilder, initialCall : Boolean, attrs : List[String], 
    relsAttrs : List[(String, List[String])], attrSelections:List[List[ASTCriterion]], 
    aggregate:Boolean, lastBag:Boolean, equivalenceClasses:EquivalenceClasses, name:String, 
    yanna:List[(String, String)], processed:List[String], resultProcessed:List[String], 
    resultAttrs:List[String], new_data_for_head_in:Boolean) : Unit = {
    
    if (attrs.isEmpty) return

    val agg_in = (aggregate && lastBag) || (aggregate && !resultAttrs.contains(attrs.head))
    val currAttr = attrs.head
    val init_agg_values = if(resultAttrs.contains(currAttr) && aggregate && 
      ((resultProcessed.size+1) == resultAttrs.size) ) true else false
    val new_data_for_head = if(processed.size == 0) (attrSelections.map(_.size).reduce( (a,b) => a + b) > 0) && resultAttrs.size == 1 && attrs.size > 1 else new_data_for_head_in
    if (attrs.tail.isEmpty) {
      emitAttrIntersection(s, true, currAttr, attrSelections.head, relsAttrs, agg_in, equivalenceClasses,name,yanna,processed,resultProcessed,resultAttrs,init_agg_values,lastBag,new_data_for_head)
    } else {
      val (updatedRelsAttrs,alloc_buffer) = emitAttrIntersection(s, false, currAttr, attrSelections.head, relsAttrs, agg_in, equivalenceClasses,name,yanna,processed,resultProcessed,resultAttrs,init_agg_values,lastBag,new_data_for_head)
      emitAttrLoopOverResult(s, initialCall, attrs, updatedRelsAttrs, attrSelections, aggregate, lastBag, equivalenceClasses,name, yanna, processed, resultProcessed, resultAttrs, init_agg_values,alloc_buffer,new_data_for_head)
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
      //e._1._1 is the encoding
      //e._1._2 is the type
      //e._2 is the map we take this and map the keys to the old index (position 4 in the map)
      (e._1._1,(e._1._2,e._2.keys.toList.sortBy{a => e._2(a)._2}.map{t => (t._1,e._2(t)._4)}))
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
    s.println(s"""auto trie_building = debug::start_clock();""")
    relsAttrs.foreach{(relAttrs : Relation) => 
      s.println(s"""Trie<${layout}> *T${relAttrs.name} = Trie<${layout}>::build(E${relAttrs.name}, [&](size_t index){(void) index; return true;});""") 
      s.println(s"""tries["${relAttrs.name}"] = T${relAttrs.name};""")
      s.println(s"""encodings["${relAttrs.name}"] = encodings_${relAttrs.name};""") 
    }
    s.println(s"""debug::stop_clock("TRIE BUILDING",trie_building);""")
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

  def emitNPRR(s:CodeStringBuilder, current:GHDNode, attribute_ordering:List[String], 
    attrSelections:List[List[ASTCriterion]], aggregate:Boolean, lastBag:Boolean, 
    equivalenceClasses:EquivalenceClasses, resultAttrs:List[String]) : Unit = {
    
    println("Bag ordering")
    current.attribute_ordering = attribute_ordering
    attribute_ordering.foreach{
      println
    }
    println("RESULT ATTRIBUTES")
    resultAttrs.foreach{println}

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
    s.println("//////////NPRR")
    if(work_to_do || aggregate){
      s.println(s"""par::reducer<size_t> ${name}_cardinality(0,[](size_t a, size_t b){""")
      s.println("return a + b;")
      s.println("});")
      if(!(lastBag && aggregate))
        s.println(s"""TrieBlock<${layout},size_t> *${name}_block;""")
      s.println("{")
      val firstBlockOfTrie = current.rels.map(( rel: Relation) => ("T" + rel.name + "->head", rel.attrs.sortBy(attribute_ordering.indexOf(_))))
      emitNPRR(s, true, attribute_ordering, firstBlockOfTrie, attrSelections, aggregate, lastBag, equivalenceClasses,name,yanna,List(),List(),resultAttrs.filter(attribute_ordering.contains(_)),false)
      s.println("}")
      s.println(s"query_result = ${name}_cardinality.evaluate(0);")
      s.println(s"std::cout << ${name}_cardinality.evaluate(0) << std::endl;")
    } else{
      val r_name = current.rels.head.name
      s.println(s"""TrieBlock<${layout},size_t> *${name}_block = T${r_name}->head;""")
    }
  }

  //cur checks should contain Block Name and Acessor
  private def emitChecks(s:CodeStringBuilder, curChecks:List[(String,String,String)], next_level_in:(String,String,String), encodingName:String, firstLoop:Boolean) : Boolean = {
    val dec_names = curChecks.map{cc =>
      if(!firstLoop){
        s.println(s"""TrieBlockIterator<${layout},size_t> ${cc._2}_${cc._1}_block_iterator = ${cc._3};""")
        s.println(s"""TrieBlock<${layout},size_t>* ${cc._2}_${cc._1}_block = ${cc._2}_${cc._1}_block_iterator.trie_block;""")
      } else{
        //Because you can't have the iterators outside of the parallel loop (will thrash caches)
        s.println(s"""TrieBlock<${layout},size_t>* ${cc._2}_${cc._1}_block = ${cc._3};""")
        s.println(s"""TrieBlockIterator<${layout},size_t> ${cc._2}_${cc._1}_block_iterator(${cc._2}_${cc._1}_block);""")
      }
      s"""${cc._2}_${cc._1}_block"""
    }
    var next_level = ""
    if(next_level_in != ("","","") ){
      s.println(s"""TrieBlock<${layout},size_t>* ${next_level_in._2}_${next_level_in._1}_block = ${next_level_in._3};""")
      next_level = s"""${next_level_in._2}_${next_level_in._1}_block"""
    }
    
    val next = if(next_level != "") List[String](next_level)++dec_names else dec_names     
    if(next.size > 0){
      s.print(s"""if(${next.head} != NULL""")
      next.tail.foreach{pc => s.print(s""" && ${pc} != NULL""")}
      s.println(s"""){""")
    }
    return next.size > 0
  }

  def emitTrie(s:CodeStringBuilder,trie:List[(String,String,String,List[(String,String,String)])],
    tries:List[List[(String,String,String,List[(String,String,String)])]],eq:EquivalenceClasses,
    prevBlock:String,prevAttr:String,first:Boolean,setBlock:Boolean,seen:mutable.Set[String],
    aggregate:Boolean, resultAttrs:List[String], aggregates:List[String], lhs_name:String) : Unit = {

    println("SEEN: " + seen)

    if(trie.size > 0 && tries.size > 1){
      val tl = trie.head
      var checksTrue = false
      val name = if(!first) s"""${tl._1}_${tl._3}""" else s"""${tl._1}"""
      val notSeen = !seen.contains(tl._3)
      seen += tl._3

      if(notSeen){
        val (encodingMap,attrMap,encodingNames,attributeToType) = eq
        val encodingName = encodingNames(attrMap(tl._3))
        val last_level_with_more = ((trie.size == 1) && (tries.tail.size > 0)) && !setBlock

        if(!aggregate){
          if(setBlock || last_level_with_more){
            s.println(s"""TrieBlock<${layout},size_t>* new_${name}_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<${layout},size_t>))) TrieBlock<${layout},size_t>(${name}_block);""")
            s.println(s"""new_${name}_block->init_pointers(tid,&output_buffer,${name}_block->set.cardinality,${encodingName}_encoding->num_distinct,true);""")
            s.println(s"""${prevBlock}->set_block(${prevAttr}_i,${prevAttr}_d,new_${name}_block);""")
          } else {
            s.println(s"""TrieBlock<${layout},size_t>* new_${name}_block = ${name}_block;""")
          }
        }
        s.println(s"""${name}_block->set.foreach_index([&](uint32_t ${tl._3}_i, uint32_t ${tl._3}_d){""")

        val next_level_in = if(trie.tail.size > 0) (trie.tail.head._3,trie.tail.head._1,trie.tail.head._2) else ("","","")
        checksTrue = emitChecks(s,tl._4,next_level_in,encodingName,false)
      }
      
      val next_prev_block = if(notSeen) s"""new_${name}_block""" else prevBlock
      val next_prev_attr = if(notSeen) tl._3 else prevAttr

      if(trie.size == 1){
        emitTrie(s,tries.tail.head,tries.tail,eq,next_prev_block,next_prev_attr,true,true,seen,aggregate,resultAttrs,aggregates,lhs_name)
      } else{
        emitTrie(s,trie.tail,tries,eq,next_prev_block,next_prev_attr,false,setBlock,seen,aggregate,resultAttrs,aggregates,lhs_name)
      }

      if(notSeen){
        if(checksTrue){
          s.println("""}""")
          if(!aggregate)
            s.println(s"""else{new_${name}_block->set_block(${tl._3}_i,${tl._3}_d,NULL);}""")
        }
        s.println("""});""")
      }
    } else if(tries.size == 1 && trie.size > 0){
      val tl = trie.head
      val name = if(!first) s"""${tl._1}_${tl._3}""" else s"""${tl._1}"""
      val notSeen = !seen.contains(tl._3)
        seen += tl._3
      
      if(notSeen && !aggregate){
        s.println(s"""${prevBlock}->set_block(${prevAttr}_i,${prevAttr}_d,${name}_block);""")
      } else{
        emitTrie(s,trie.tail,tries,eq,prevBlock,prevAttr,false,false,seen,aggregate,resultAttrs,aggregates,lhs_name)
      }
    } else{
      if(aggregate){
        s.print(s"""size_t count = """)
        s.print(s"""${aggregates.head}""")
        aggregates.tail.foreach(agg => s.print(s"""*${agg}"""))
        s.println(s""";""")
        s.println(s"""${lhs_name}_cardinality.update(tid,count);""")
      }
    }


  }

  def emitTopDown(s:CodeStringBuilder, accessor:(List[String],List[String],List[(List[(String,String,String,List[(String,String,String)])])],List[String]), eq:EquivalenceClasses, aggregate:Boolean, resultAttrs:List[String]) = {
    val (preChecks,resultAttrs,trieStrings,aggregates) = accessor
    val lhs_name = lhs.identifierName + "_" + resultAttrs.mkString("")
    val (encodingMap,attrMap,encodingNames,attributeToType) = eq
    s.println(s"""////////////TOP DOWN""")

    s.println(s"""par::reducer<size_t> ${lhs_name}_cardinality(0,[](size_t a, size_t b){""")
    s.println("return a + b;")
    s.println("});")

    //First check that all tries exist = prechecks
    s.println(s"""TrieBlock<${layout},size_t> *${lhs_name}_block = NULL;""")
    if(preChecks.size != 0){
      s.print(s"""if(${preChecks.head} != NULL""")
      preChecks.tail.foreach{pc => s.print(s""" && ${pc} != NULL""")}
      s.println("""){""")
    }

    //emit parallel loop
    val curTrie = trieStrings.head
    val headBlock = curTrie.head

    val last_level_with_more = ((curTrie.size == 1) && (trieStrings.tail.size > 0))
    if(last_level_with_more && !aggregate){
      val (encodingMap,attrMap,encodingNames,attributeToType) = eq
      val encodingName = encodingNames(attrMap(headBlock._3))
      s.println(s"""${lhs_name}_block = new (output_buffer.get_next(0, sizeof(TrieBlock<${layout},size_t>))) TrieBlock<${layout},size_t>(${headBlock._2});""")
      s.println(s"""${lhs_name}_block->init_pointers(0,&output_buffer,${lhs_name}_block->set.cardinality,${encodingName}_encoding->num_distinct,true);""")
    } else{
      s.println(s"""${lhs_name}_block = ${headBlock._2};""")
    }
    s.println(s"""${lhs_name}_block->set.par_foreach_index([&](size_t tid, uint32_t ${headBlock._3}_i, uint32_t ${headBlock._3}_d){""")
    preChecks.foreach{pc => s.println(s"""TrieBlockIterator<${layout},size_t> ${pc}_iterator(${pc});""")}

    val curChecks = headBlock._4
    val encodingName = encodingNames(attrMap(headBlock._3))
    val next_level_in = if(curTrie.tail.size > 0) (curTrie.tail.head._3,curTrie.tail.head._1,curTrie.tail.head._2) else ("","","")
    val checksTrue = emitChecks(s,curChecks,next_level_in,encodingName,true)

    if(curTrie.size == 1){
      emitTrie(s,trieStrings.tail.head,trieStrings.tail,eq,lhs_name + "_block",headBlock._3,true,true,mutable.Set[String](headBlock._3),aggregate,resultAttrs,aggregates,lhs_name)
    } else{
      emitTrie(s,curTrie.tail,trieStrings,eq,lhs_name + "_block",headBlock._3,false,false,mutable.Set[String](headBlock._3),aggregate,resultAttrs,aggregates,lhs_name)
    }

    //Close off first attribute
    if(checksTrue){
      s.println("""}""")
      if(!aggregate)
        s.println(s"""else { ${lhs_name}_block->set_block(${headBlock._3}_i,${headBlock._3}_d,NULL); } """)
    }
    s.println("""});""")
    if(preChecks.size != 0){
      s.println("""}""")
    }
    s.println(s"""std::cout << ${lhs_name}_cardinality.evaluate(0) << std::endl;""")
    emitResultTrie(s,lhs.identifierName,lhs_name,resultAttrs,eq)
  }

  override def code(s: CodeStringBuilder): Unit = {
    //////////////////////////////////////////////////////////////////////
    //start solver stuff
    var relations = rels.map((rel : ASTRelation) => new Relation(rel.attrs, rel.identifierName))
    val solver = GHDSolver
    //get minimum GHD's
    val myghd = solver.getGHD(relations)
    val attribute_ordering = solver.getAttributeOrdering(myghd,lhs.attrs)
    val resultAttrs = if(aggregate) List[String]() else lhs.attrs.sortBy(attribute_ordering.indexOf(_))
    //////////////////////////////////////////////////////////////////////
    println("RESULT ATTRIBUTES")
    resultAttrs.foreach{
      println
    }
    println("Attribute Ordering")
    attribute_ordering.foreach{println}
    /**
     * We get a distinct list of them so we can look them up and have a pointer to them
     */
    val reordered_relations = rels.map((rel : ASTRelation) => new Relation(rel.attrs.sortBy(e => attribute_ordering.indexOf(e)), rel.identifierName))
    val distinctRelations = reordered_relations.groupBy(_.name).map(_._2.head).toList.sortBy(a => a.name) //distinct
    emitRelationLookupAndCast(s, distinctRelations.map(_.name))

    /**
     * Now emit the encodings of each of the attrs
     */
    val equivalenceClasses = buildEncodingEquivalenceClasses(relations,reordered_relations) //use old relation orderings to build
    s.println(s"""auto encoding_time =  debug::start_clock();""")
    emitEncodingForEquivalenceClasses(s, equivalenceClasses, distinctRelations) //use new relation orderings to pull out
    s.println(s"""debug::stop_clock("ENCODING TIME",encoding_time);""")

    /**
     * emit the trie building
     */
    emitTrieBuilding(s, distinctRelations)

    /**
     * Emit the buffers that we will do intersections for each attr in
     */
    val (e_to_index2,attributeToEncoding,encodingIDToName,attributeToType) = equivalenceClasses
    val size_string = encodingIDToName.head._2 + "_encoding->num_distinct" + encodingIDToName.tail.map{ e =>
      s"""+${e._2}_encoding->num_distinct"""
    }.mkString("")
    s.println(s"""allocator::memory<uint8_t> output_buffer(${myghd.num_bags}*${attribute_ordering.size}*10*sizeof(TrieBlock<${layout},size_t>)*(${size_string}));""")
    s.println(s"""allocator::memory<uint8_t> tmp_buffer(${myghd.num_bags}*${attribute_ordering.size}*2*sizeof(TrieBlock<${layout},size_t>)*(${size_string}));""")

    //Prepare the attributes that will need to be selected on the fly
    val attrSelections = emitAndDetectSelections(s,attribute_ordering,equivalenceClasses)

    val top_down_unnecessary = myghd.children.size == 0 || (!aggregate && resultAttrs.foldLeft(true){ (a,b) => (myghd.attribute_ordering.indexOf(a) == resultAttrs.indexOf(a)) && (myghd.attribute_ordering.indexOf(b) == resultAttrs.indexOf(b))})
    println("TOPD DOWN UNECCESSARY: " + top_down_unnecessary)

    s.println(s"""auto join_timer = debug::start_clock();""")
    val emptyGHD = new GHDNode(List[Relation]())
    val new_agg_attributes = solver.bottom_up(mutable.LinkedHashSet[GHDNode](myghd), myghd, emitNPRR, s, attribute_ordering, attrSelections, aggregate, top_down_unnecessary, equivalenceClasses, resultAttrs, emptyGHD)
    if(!top_down_unnecessary){
      val resultAttrsIn = if(!aggregate) resultAttrs else new_agg_attributes
      val accessor = solver.top_down(mutable.LinkedHashSet[GHDNode](myghd), mutable.LinkedHashSet(myghd),resultAttrsIn)
      emitTopDown(s,accessor,equivalenceClasses, aggregate, resultAttrsIn) 
    } else if(!aggregate){
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
