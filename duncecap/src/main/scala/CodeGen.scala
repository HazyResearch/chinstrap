package DunceCap

import scala.collection.mutable
/**
 * All code generation methods start from this object:
 */
object CodeGen {
  val annotationType = "long"
  def emitCode(s: CodeStringBuilder, root: List[ASTNode]) = {
    emitHeader(s,root)
    emitMainMethod(s)
  }

  def emitHeader(s:CodeStringBuilder, root:List[ASTNode]) = {
    s.println("#define GENERATED")
    s.println("#include \"main.hpp\"")
    s.println(s"""extern \"C\" void* run(std::unordered_map<std::string, void*>& relations, std::unordered_map<std::string, Trie<${Environment.layout},${annotationType}>*> tries, std::unordered_map<std::string, std::vector<void*>*> encodings) {""")
    s.println("""(void) relations; (void) tries; (void) encodings; //TODO keep in memory across repl calls""")
    root.foreach{_.code(s)}
    s.println("return NULL;")
    s.println("}")  
  }
  /**
    * emitMainMethod will get called only in compiler mode
    */
  def emitMainMethod(s : CodeStringBuilder): Unit = {
    s.println("#ifndef GOOGLE_TEST")
    s.println("int main() {")
    s.println("std::unordered_map<std::string, void*> relations;")
    s.println(s"std::unordered_map<std::string, Trie<${Environment.layout},${annotationType}>*> tries;")
    s.println(s"std::unordered_map<std::string, std::vector<void*>*> encodings;")
    s.println("thread_pool::initializeThreadPool();")
    s.println("run(relations,tries,encodings);")
    s.println("}")
    s.println("#endif")
  }

  def emitInitCreateDB(s:CodeStringBuilder): Unit = {
    s.println("""////////////////////emitInitCreateDB////////////////////""")
    s.println("""//init relations""")
    Environment.relations.foreach( tuple => {
      val (name,rmap) = tuple
      val typeString = rmap.head._2.types.mkString(",")
      s.println(s"""Relation<${typeString}>* ${name} = new Relation<${typeString}>();""")
    })

    s.println("""//init encodings""")
    Environment.encodings.foreach(tuple => {
      val (name,encoding) = tuple
      s.println(s"""SortableEncodingMap<${encoding._type}>* ${encoding.name}_encodingMap = new SortableEncodingMap<${encoding._type}>();""")
      s.println(s"""Encoding<${encoding._type}>* Encoding_${encoding.name} = new Encoding<${encoding._type}>();""")
    })
    s.print("\n")
  }

  def emitLoadRelation(s: CodeStringBuilder,sourcePath:String,rel:Relation): Unit = {
    s.println("""////////////////////emitLoadRelation////////////////////""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    val codeString = s"""tsv_reader f_reader("${sourcePath}");
                         char *next = f_reader.tsv_get_first();
                         while(next != NULL){ """
    s.println(codeString)
    for (i <- 0 until rel.types.length){
      s.println(s"""${rel.encodings(i)}_encodingMap->update(${rel.name}->append_from_string<${i}>(next));""") 
      s.println(s"""next = f_reader.tsv_get_next();""")
    }
    s.println(s"""${rel.name}->num_rows++; }""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("READING RELATION ${rel.name}",start_time);""")
    s.println("}")
    s.print("\n")
  }

  def emitBuildEncodings(s: CodeStringBuilder): Unit = {
    s.println("""////////////////////emitBuildEncodings////////////////////""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    Environment.encodings.foreach(tuple => {
      val (name,encoding) = tuple
      s.println(s"""Encoding_${name}->build(${name}_encodingMap);""")
      s.println(s"""delete ${name}_encodingMap;""")
    })
    if(!Environment.quiet) s.println(s"""debug::stop_clock("BUILDING ENCODINGS",start_time);""")
    s.println("} \n")
  }

  def emitReorderEncodedRelation(s: CodeStringBuilder,rel:Relation,name:String,order:List[Int],masterName:String): Unit = {
    s.println("""////////////////////emitReorderEncodedRelation////////////////////""")
    s.println(s"""EncodedRelation* Encoded_${rel.name} = new EncodedRelation();""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println("//encodeRelation")
    for (i <- 0 until order.length){
      s.println(s"""Encoded_${rel.name}->add_column(Encoded_${masterName}->column(${order(i)}),Encoded_${masterName}->max_set_size.at(${order(i)}));""")
    }
    if(!Environment.quiet) s.println(s"""debug::stop_clock("REORDERING ENCODING ${rel.name}",start_time);""")
    s.println("} \n")
  }

  def emitEncodeRelation(s: CodeStringBuilder,rel:Relation): Unit = {
    val name = rel.name
    s.println("""////////////////////emitEncodeRelation////////////////////""")
    s.println(s"""EncodedRelation* Encoded_${rel.name} = new EncodedRelation();""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println("//encodeRelation")
    for (i <- 0 until rel.types.length){
      s.println(s"""Encoded_${rel.name}->add_column(Encoding_${rel.encodings(i)}->encode_column(&${name}->get<${i}>()),Encoding_${rel.encodings(i)}->num_distinct);""")
    }
    s.println(s"""Encoded_${rel.name}->to_binary("${Environment.dbPath}/relations/${rel.name}/");""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("ENCODING ${rel.name}",start_time);""")
    s.println("} \n")
  }
  
  def emitWriteBinaryTrie(s: CodeStringBuilder,rel:String,relName:String): Unit = {
    s.println("""////////////////////emitWriteBinaryTrie////////////////////""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println(s"""Trie_${relName}->to_binary("${Environment.dbPath}/relations/${rel}/${relName}/");""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("WRITING BINARY TRIE ${relName}",start_time);""")
    s.println("} \n")
  }

  def emitBuildTrie(s: CodeStringBuilder,rel:Relation,relName:String): Unit = {
    s.println("""////////////////////emitBuildTrie////////////////////""")
    s.println(s"""const size_t alloc_size_${rel.name} = 8*Encoded_${rel.name}->data.size()*Encoded_${rel.name}->data.at(0).size()*sizeof(uint64_t)*sizeof(TrieBlock<${Environment.layout},${annotationType}>);""")
    s.println(s"""allocator::memory<uint8_t>* data_allocator_${rel.name} = new allocator::memory<uint8_t>(alloc_size_${rel.name});""")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${rel.name} = NULL;""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println("//buildTrie")
    s.println(s"""Trie_${rel.name} = Trie<${Environment.layout},${annotationType}>::build(data_allocator_${rel.name},&Encoded_${rel.name}->max_set_size,&Encoded_${rel.name}->data,[&](size_t index){ (void) index; return true;});""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("BUILDING TRIE ${rel.name}",start_time);""")
    s.println("} \n")
    emitWriteBinaryTrie(s,relName,rel.name)
    s.println(s"""data_allocator_${rel.name}->free();""")
    s.println(s"""delete data_allocator_${rel.name};""")
    s.println(s"""delete Trie_${rel.name};""")
  }

  def emitLoadEncodedRelation(s: CodeStringBuilder,rel:Relation): Unit = {
    s.println("""////////////////////emitLoadEncodedRelation////////////////////""")
    s.println(s"""EncodedRelation* Encoded_${rel.name} = NULL;""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println(s"""Encoded_${rel.name} = EncodedRelation::from_binary("${Environment.dbPath}/relations/${rel.name}/");""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("LOADING ENCODED RELATION ${rel.name}",start_time);""")
    s.println("} \n")
  }

  def emitWriteBinaryEncoding(s: CodeStringBuilder,enc:Encoding): Unit = {
    s.println("""////////////////////emitWriteBinaryEncoding////////////////////""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println(s"""Encoding_${enc.name}->to_binary("${Environment.dbPath}/encodings/${enc.name}/");""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("WRITING ENCODING ${enc.name}",start_time);""")
    s.println("} \n")
  }
 
  def emitLoadBinaryEncodings(s: CodeStringBuilder,encodings:List[String]): Unit = {
    s.println("""////////////////////emitLoadBinaryEncoding////////////////////""")
    if(!Environment.quiet) s.println("""auto belt = debug::start_clock();""")
    encodings.foreach(e => {
      s.println(s"""Encoding<${Environment.encodings(e)._type}>* Encoding_${e} = Encoding<${Environment.encodings(e)._type}>::from_binary("${Environment.dbPath}/encodings/${e}/");""")
      s.println(s"""(void) Encoding_${e};""")
    })
    if(!Environment.quiet) s.println(s"""debug::stop_clock("LOADING ENCODINGS",belt);""")
    s.print("\n")
  }
 
  def emitLoadBinaryRelations(s: CodeStringBuilder,relations:List[(String,String)]): Unit = {
    s.println("""////////////////////emitLoadBinaryRelation////////////////////""")
    if(!Environment.quiet) s.println("""auto btlt = debug::start_clock();""")
    relations.foreach(qr => {
      s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${qr._2} = Trie<${Environment.layout},${annotationType}>::from_binary("${Environment.dbPath}/relations/${qr._1}/${qr._2}/");""")
    })
    if(!Environment.quiet) s.println(s"""debug::stop_clock("LOADING RELATIONS",btlt);""")
    s.print("\n")
  }

  def emitAllocators(s:CodeStringBuilder) : Unit = {
    s.println("""////////////////////emitAllocators////////////////////""")
    s.println(s"""allocator::memory<uint8_t> *output_buffer = new allocator::memory<uint8_t>(10000);""")
    s.println(s"""allocator::memory<uint8_t> *tmp_buffer = new allocator::memory<uint8_t>(10000); (void) tmp_buffer;""")
  }

  def emitPrintRelation(s: CodeStringBuilder,rel:QueryRelation): Unit = {
    s.println("""////////////////////emitPrintRelation////////////////////""")
    s.println("{")
    //load from binary first
    val qr = rel.name + "_" + (0 until rel.attrs.size).toList.mkString("_")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${qr} = Trie<${Environment.layout},${annotationType}>::from_binary("${Environment.dbPath}/relations/${rel.name}/${qr}/");""")
    val loadEncodings = (0 until rel.attrs.size).map(i => { Environment.relations(rel.name)(qr).encodings(i)}).toList.distinct
    emitLoadBinaryEncodings(s,loadEncodings)

    s.println(s"""Trie_${qr}->foreach([&](std::vector<uint32_t>* tuple){""")
    (0 until rel.attrs.size).foreach(i => {
      val encodingName = Environment.relations(rel.name)(qr).encodings(i)
      s.println(s"""std::cout << Encoding_${encodingName}->key_to_value.at(tuple->at(${i})) << "\t" << " "; """)
    })
    s.println(s"""std::cout << std::endl;""")
    s.println("});")
    s.println("} \n")
  }

  def emitTrieBlock(s:CodeStringBuilder,attr:String,accessor:Accessor): Unit = {
    s.print(s"""const TrieBlock<${Environment.layout},${annotationType}>* ${accessor.getName()} = """)
    val getBlock = if(accessor.level == 0) 
        s"""Trie_${accessor.trieName}->head;""" 
      else 
        s"""${accessor.getPrevName()}->get_block(${accessor.getPrevAttr()}_d);"""

    s.println(getBlock)
  }

  def emitSelectedTrieBlock(s:CodeStringBuilder,attr:String,accessor:Accessor): Unit = {
    s.println("//emitSelectedTrieBlock")
    s.print(s"""const TrieBlock<${Environment.layout},${annotationType}>* ${accessor.getName()} = """)
    val getBlock = if(accessor.level == 0) 
        s"""Trie_${accessor.trieName}->head;""" 
      else 
        s"""${accessor.getPrevName()}->get_block(${accessor.getPrevAttr()}_selection_data_index,${accessor.getPrevAttr()}_selection);"""

    s.println(getBlock)
  }

  def emitMaxSetAlloc(s:CodeStringBuilder,attr:String,accessors:List[Accessor]) : Unit = {
    if(accessors.length > 1){
      s.print(s"""const size_t alloc_size_${attr} = """)
      accessors.tail.foreach(ac => { 
        s.print(s"""std::max(${ac.getName()}->set.number_of_bytes,""" ) 
      })
      s.print(s"""${accessors.head.getName()}->set.number_of_bytes""")
      accessors.tail.foreach(ac => s.print(")"))
      s.print(";")
    }
  }

  def emitIntersection(s:CodeStringBuilder,resultSet:String,op1:String,op2:String,selection:Option[SelectionCondition],attr:String) : Unit = {
    s.println(s"""${resultSet} = *ops::set_intersect(&${resultSet},
      (const Set<${Environment.layout}> *)&${op1},
      (const Set<${Environment.layout}> *)&${op2}""")
    selection match {
      case Some(sel) => {
        s.print(s""",[&](uint32_t ${attr}_data, uint32_t _1, uint32_t _2){ return """)
        val condition = if(sel.condition == "=") "==" else sel.condition
        s.println(s"""(void) _1; (void) _2; ${attr}_selection ${condition} ${attr}_data;})""")
      }
      case None =>
    }
    s.println(""");""")
  }

  def emitAnnotationInitialization(s:CodeStringBuilder,attr:String,annotation:Annotation,tid:String,scalarResult:Boolean): Unit = {
    if(!annotation.next.isDefined && annotation.passedAnnotations.length == 0){ //always the last attribute?
      val exp2 = if(!annotation.prev.isDefined) "(" else s"""(annotation_tmp * """
      s.println(s"""${annotationType} annotation_${attr} = ${exp2} (${annotation.expression}*${attr}.cardinality));""")
    }else {
      if(scalarResult && !annotation.prev.isDefined) emitAggregateReducer(s,annotation.operation)
      else if(!annotation.prev.isDefined) s.println(s"""${annotationType} annotation_${attr} = (${annotationType})0;""")    
      else s.println(s"""${annotationType} annotation_${attr} = (${annotationType})0;""")            
    }
  }

  def emitMaterializedSelection(s:CodeStringBuilder,attr:String,selectionsBelow:Boolean,tid:String,selection:SelectionCondition,loopSet:String) : Unit = {
    s.println(s"""//emitMaterializedSelection""")
    val setName = if(selectionsBelow) s"""${attr}_filtered""" else attr

    s.println(s"""size_t ${attr}_s_i = 0;""")
    s.println(s"""uint32_t* ${attr}_data = (uint32_t*)output_buffer->get_next(${tid},${loopSet}.number_of_bytes);""")
    s.println(s"""${loopSet}.foreach([&](uint32_t ${attr}_s_d){""")
    val condition = if(selection.condition == "=") "==" else selection.condition
    s.println(s"""if(${attr}_s_d ${condition} ${attr}_selection){""")
    s.println(s"""${attr}_data[${attr}_s_i++] = ${attr}_s_d;""")
    s.println(s"""}""")
    s.println(s"""});""")
    s.println(s"""const size_t ${attr}_data_range = (${attr}_s_i > 0) ? (${attr}_data[${attr}_s_i-1]-${attr}_data[0]) : 0;""")
    s.println(s"""Set<${Environment.layout}> ${setName}((uint8_t*)${attr}_data,${attr}_s_i,${attr}_data_range,${attr}_s_i*sizeof(uint32_t),type::UINTEGER);""")
  }

  def emitSetupFilteredSet(s:CodeStringBuilder,first:Boolean,attr:String,tid:String) : Unit = {
    if(first)
      s.println(s"""std::atomic<size_t> filter_index(0);""")
    else 
      s.println(s"""size_t filter_index = 0;""")

    s.println(s"""uint32_t* filtered_data = (uint32_t*)output_buffer->get_next(${tid},${attr}_filtered.cardinality*sizeof(uint32_t));""")
  }

    //the materialized set condition could be nested so we want a GOTO to break out once a single cond is met
    //to do this will only work with one nested condition.
  def emitCheckConditions(s:CodeStringBuilder,selection:SelectionCondition,attr:String,loopSet:String) : Unit = {
    s.println(s"""//emitCheckConditions""")
    selection.condition match {
      case "=" => {
        s.println(s"""const long ${attr}_selection_data_index = ${loopSet}.find(${attr}_selection);""")
        s.println(s"""if(${attr}_selection_data_index != -1){""")
      } case _ => throw new IllegalStateException("Selection condition not allowed")
    }
  }

    //the materialized set condition could be nested so we want a GOTO to break out once a single cond is met
    //to do this will only work with one nested condition.
  def emitFinalCheckConditions(s:CodeStringBuilder,selection:SelectionCondition,attr:String,loopSet:String,first:Boolean,prevAttr:String) : Unit = {
    s.println(s"""//emitCheckConditions""")
    val fi = if(first) "filter_index.fetch_add(1)" else "filter_index++"
    selection.condition match {
      case "=" => {
        s.println(s"""const long ${attr}_selection_data_index = ${loopSet}.find(${attr}_selection);""")
        s.println(s"""if(${attr}_selection_data_index != -1) filtered_data[${fi}] = ${prevAttr}_d;""")
      } case _ => throw new IllegalStateException("Selection condition not allowed")
    }
  }

  def emitRollBack(s:CodeStringBuilder,attr:String,selectionBelow:Boolean,tid:String):Unit = {
    if(selectionBelow){
      s.println(s"""tmp_buffer->roll_back(${tid},alloc_size_${attr}-${attr}_filtered.number_of_bytes);""")
    } else{
      s.println(s"""output_buffer->roll_back(${tid},alloc_size_${attr}-${attr}.number_of_bytes);""")
    }
  }

  def emitNewTrieBlock(s:CodeStringBuilder,attr:String,tid:String,last:Boolean,annotated:Boolean): Unit = {
    s.println("//emitNewTrieBlock")
    s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* TrieBlock_${attr} = 
      new(output_buffer->get_next(${tid}, sizeof(TrieBlock<${Environment.layout},${annotationType}>))) 
      TrieBlock<${Environment.layout},${annotationType}>(${attr});""")
    if(annotated){
      s.println(s"""TrieBlock_${attr}->init_pointers_and_data(${tid},output_buffer);""")
    } else if(!last){
      s.println(s"""TrieBlock_${attr}->init_pointers(${tid},output_buffer);""")
    }
  }

  def emitSetTrie(s:CodeStringBuilder,lhs:String,attr:String) : Unit = {
    s.println(s"""Trie_${lhs}->head = TrieBlock_${attr};""")
  }

  def emitSetTrieBlock(s:CodeStringBuilder,attr:String,lastMaterialized:Boolean,nextMaterialized:Option[String],lhs:String,setData:Option[String]): Unit = {
    s.println("//emitSetTrieBlock")
    if(!lastMaterialized){
      nextMaterialized match {
        case Some(a) =>
          s.println(s"""TrieBlock_${attr}->set_block(${attr}_i,${attr}_d,TrieBlock_${a});""")
        case None =>
      }
    } else if(setData.isDefined){
      s.println(s"""TrieBlock_${attr}->set_data(${attr}_i,${attr}_d,annotation_${setData.get});""")
    }
  }

  def emitAggregateReducer(s:CodeStringBuilder,operation:String):Unit = {
    s.println("//emitAggregateReducer")
    operation match {
      case "SUM" => 
        s.println(s"""par::reducer<${annotationType}> annotation(0,[](size_t a, size_t b){ return a + b; });""")
      case _ =>
        throw new IllegalStateException("Aggregate: " + operation + " is NOT IMPLEMENTED");
    }
  }

  def emitScalarAnnotation(s:CodeStringBuilder,lhs:String): Unit = {
    s.println(s"""Trie_${lhs}->annotation = annotation.evaluate(0);""")
  }

  def emitNoMaterializeCheckSelection(s:CodeStringBuilder,cga:CodeGenNPRRAttr) : Unit = {
    if(cga.accessors.length == 1 && !cga.materialize && !cga.last){
      //Check a selection but we will not materialize it and no intersections performed filter on set we are looping over
      cga.selection match {
        case Some(selection) => {
          val condition = if(selection.condition == "=") "==" else selection.condition
          s.println(s"""if(${cga.attr}_d ${condition} ${cga.attr}_selection){""")
        }
        case None =>
      }
    }
  }

  def emitNoMaterializeCheckSelectionEnd(s:CodeStringBuilder,cga:CodeGenNPRRAttr) : Unit = {
    if(cga.accessors.length == 1 && !cga.materialize && !cga.last){
      //Check a selection but we will not materialize it and no intersections performed filter on set we are looping over
      cga.selection match {
        case Some(selection) => {
          s.println("}")
          s.println("return ret_value;")
        }
        case None =>
      }
    }
  }

  def emitForEach(s:CodeStringBuilder,setName:String,attr:String,first:Boolean) : Unit = {
    if(first){
      s.println(s"""${setName}.par_foreach([&](size_t tid, uint32_t ${attr}_d){ (void) tid;""")
    } else{
      s.println(s"""${setName}.foreach([&](uint32_t ${attr}_d) {""")
    }
  }

  def emitForEachIndex(s:CodeStringBuilder,attr:String,first:Boolean) : Unit = {
    if(first){
      s.println(s"""${attr}.par_foreach_index([&](size_t tid, uint32_t ${attr}_i, uint32_t ${attr}_d){ (void) tid;""")
    } else{
      s.println(s"""${attr}.foreach_index([&](uint32_t ${attr}_i, uint32_t ${attr}_d) {""")
    }
  }

  def emitRewriteOutputTrie(s:CodeStringBuilder,outName:String,prevName:String,scalarResult:Boolean) : Unit = {
    s.println("//emitRewriteOutputTrie")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${outName} = Trie_${prevName};""")
  }

  def emitUpdatePassedAttributes(s:CodeStringBuilder,attr:String,operation:String,passedAnnotations:List[Accessor]) : Unit = {
    operation match {
      case "SUM" => {
        s.println(s"""annotation_${attr} += """)
      }
      case _ => throw new IllegalStateException("Op not defined")
    }
    s.println(s"""(annotation_tmp * ${passedAnnotations.head.getName()}->get_data(${attr}_d)""")
    passedAnnotations.tail.foreach(pa => {
      s.println(s"""*${pa.getName()}->get_data(${attr}_d)""")
    })
    s.println(");")
  }

  def emitAnnotationTemporary(s:CodeStringBuilder, attr:String, annotation:Annotation) : Unit = {
    if(annotation.next.isDefined && annotation.passedAnnotations.length != 0){ //not the last loop and we have passed annotations
      if(!annotation.prev.isDefined)
        s.println(s"""${annotationType} annotation_tmp = (${annotation.expression}*(""")
      else 
        s.println(s"""annotation_tmp *= (${annotation.expression}*(""")

      s.println(s"""${annotation.passedAnnotations.head.getName()}->get_data(${attr}_d)""")
      annotation.passedAnnotations.tail.foreach(pa => {
        s.println(s"""*${pa.getName()}->get_data(${attr}_d)""")
      })
      s.println("));")
    } else if(!annotation.prev.isDefined){
      s.println(s"""${annotationType} annotation_tmp = ${annotation.expression};""")
    } else {
      s.println(s"""annotation_tmp *= ${annotation.expression};""")
    }
  }

  def emitRollBackALL(s:CodeStringBuilder,attr:String,buffer:String,tid:String) : Unit = {
    s.println(s"""${buffer}->roll_back(${tid},alloc_size_${attr});""")
  }
  /*
  def emitAnnotationComputation(s:CodeStringBuilder,annotation:Annotation) : Unit = {
  }
  */
  def emitUpdateAnnotationReducer(s:CodeStringBuilder,attr:String,tid:String) : Unit = {
    s.println(s"""annotation.update(${tid},annotation_${attr});""")
  }
  def emitUpdateAnnotation(s:CodeStringBuilder,attr:String,prev:String,operation:String) : Unit = {
    operation match {
        case "SUM" => {
          s.println(s"""annotation_${prev} += annotation_${attr};""")
        }
        case _ => throw new IllegalStateException("Op not implemented")
    }
  }

  //the materialized set condition could be nested so we want a GOTO to break out once a single cond is met
  def emitBuildNewSet(s:CodeStringBuilder,first:Boolean,last:Boolean,attr:String,tid:String) : Unit = {
    s.println(s"""//emitBuildNewSet""")
    val fi = if(first) "filter_index.load()" else "filter_index"
    if(first){
      s.println(s"""tbb::parallel_sort(filtered_data,filtered_data+${fi});""");
    }
    s.println(s"""const size_t ${attr}_range = (${fi} > 0) ? (filtered_data[${fi}-1]-filtered_data[0]) : 0;""")
    s.println(s"""Set<${Environment.layout}> ${attr}((uint8_t*)filtered_data,${fi},${attr}_range,${fi}*sizeof(uint32_t),type::UINTEGER);""")
    s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* TrieBlock_${attr} = 
      new(output_buffer->get_next(${tid}, sizeof(TrieBlock<${Environment.layout},${annotationType}>))) 
      TrieBlock<${Environment.layout},${annotationType}>(${attr});""")

    if(!last)
      s.println(s"""TrieBlock_${attr}->init_pointers(${tid},output_buffer);""")
  }

  def emitStartQueryTimer(s:CodeStringBuilder) : Unit = {
    s.println("""auto query_time = debug::start_clock();""")
  }
  def emitStopQueryTimer(s:CodeStringBuilder) : Unit = {
    s.println("""debug::stop_clock("QUERY TIME",query_time);""")
  }

  def emitSelectionValue(s:CodeStringBuilder, attr:String, encoding:String, value:String) : Unit = {
    //emit the selection values first if they exist
    s.println(s"""const uint32_t ${attr}_selection = Encoding_${encoding}->value_to_key.at(${value});""")
  }

  def emitTopDown(s: CodeStringBuilder, first:Boolean, td:List[CodeGenTopDown], name:String, cg:CodeGenGHD) : Unit = {
    //First emit the blocks for the heads of all the relations
    td.foreach(cg_top_down =>{
      val attr = cg_top_down.attr
      val accessors = cg_top_down.accessors
      accessors.distinct.foreach(acc => {
        if(acc.level == 0)
          s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* ${acc.getName()} = Trie_${acc.trieName}->head;""")
      })
    })

    var prev = ""
    td.foreach(cg_top_down =>{
      val attr = cg_top_down.attr
      val accessors = cg_top_down.accessors
      val tid = if(first && cg_top_down == td.head) "0" else "tid"

      if(cg.aggregates.length != 0){
        if(attr == cg.aggregates.head){
          if(cg.scalarResult){
            s.println(s"""par::reducer<${annotationType}> annotation(0,[](size_t a, size_t b){ return a + b; });""")
          } else{
            s.println(s"""${annotationType} annotation = (${annotationType})0;""")
          }
        }
      }

      //emit remaining accessors
      accessors.foreach(acc => {
        if(acc.level != 0){
          s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* ${acc.getName()} = ${acc.getPrevName()}->get_block(${acc.attrs(acc.level-1)}_d);""")
        }
      })
      
      if(!cg.aggregates.contains(attr)){
        //emit for loop
        s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* TrieBlock_${attr} = 
          new(output_buffer->get_next(${tid}, sizeof(TrieBlock<${Environment.layout},${annotationType}>))) 
          TrieBlock<${Environment.layout},${annotationType}>(${accessors.head.getName()}->set);""")
        //last level we must start rebuilding
        if(td.last != cg_top_down){
          s.println(s"""TrieBlock_${attr}->init_pointers(${tid},output_buffer);""")
        }
      } else {
       s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* TrieBlock_${attr} = ${accessors.head.getName()};""")
      }

      if(!cg.aggregates.contains(attr)){
        if(first && cg_top_down == td.head){
          s.println(s"""TrieBlock_${attr}->set.par_foreach_index([&](size_t tid, uint32_t ${attr}_i, uint32_t ${attr}_d){""")
        } else{
          s.println(s"""TrieBlock_${attr}->set.foreach_index([&](uint32_t ${attr}_i, uint32_t ${attr}_d) {""")
        }
      } else {
        if(first && cg_top_down == td.head){
          s.println(s"""TrieBlock_${attr}->set.par_foreach([&](size_t tid, uint32_t ${attr}_d){""")
        } else{
          s.println(s"""TrieBlock_${attr}->set.foreach([&](uint32_t ${attr}_d) {""")
        }
        s.print(s"""const ${annotationType} annotation_${attr} = (""")
        accessors.filter(acc => (attr == acc.attrs.last)).foreach(acc => { //filter out those which are not annotations 
          s.println(s"""${acc.getName()}->get_data(${attr}_d)*""")
        })
        s.println("1);")
      }
      prev = attr
    })

    td.reverse.foreach(cg_top_down =>{
      val attr = cg_top_down.attr
      val accessors = cg_top_down.accessors
      if(!cg.aggregates.contains(attr)){
        if(td.last != cg_top_down){
          s.println(s"""TrieBlock_${attr}->set_block(${attr}_i,${attr}_d,TrieBlock_${prev});""")
        }
      } else {
        if(cg.annotatedAttr.isDefined){
          s.println("//set data")
        } else {
          if(cg.scalarResult && td.last == cg_top_down){
            s.println(s"""annotation.update(tid,(annotation_${attr}""")
            val index = cg.aggregates.indexOf(attr)
            (0 until index).foreach(i => {
              s.println(s"""*annotation_${cg.aggregates(i)}""")
            })
            s.println("*1));")
          }
        }
      }
      s.println("});")
      prev = attr
    })

    s.println(s"""Trie_${name}->head = TrieBlock_${td.head.attr};""")
    if(cg.scalarResult){
      s.println(s"""Trie_${name}->annotation = annotation.evaluate(0);""")
    }
  }

  def emitAllocateAggregateBuffer(s:CodeStringBuilder,attr:String) : Unit = {
    s.println(s"""allocator::memory<uint8_t> *${attr}_buffer = new allocator::memory<uint8_t>(10000);""")
  }

  def emitFreeAggregateBuffer(s:CodeStringBuilder,attr:String) : Unit = {
    s.println(s"""${attr}_buffer->free();""")
  }

  def emitAllocateResultTrie(s:CodeStringBuilder,noWork:Option[String],result:QueryRelation) : Unit = {
    s.println(s"""////////////////////NPRR BAG ${result.name}////////////////////""")

    if(noWork.isDefined){
      s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${result.name} = Trie_${noWork.get};""")
      return
    }

    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${result.name} = 
      new (output_buffer->get_next(0, sizeof(Trie<${Environment.layout}, ${annotationType}>))) 
      Trie<${Environment.layout}, ${annotationType}>(${result.attrs.length});""")
    s.println("{")

    s.println("""auto start_time = debug::start_clock();""")
  }
  def emitFinishNPRR(s:CodeStringBuilder,result:QueryRelation) : Unit = {
    s.println(s"""debug::stop_clock("Bag ${result.name}",start_time);""")
    s.println("} \n")
  }

  //top down still needs refactor
  def emitNPRR(s: CodeStringBuilder,name: String,cg:CodeGenGHD,noWork:Option[String],td:Option[List[CodeGenTopDown]]): Unit = {
    //emit top down code if you have it
    td match {
      case Some(top_down) =>{
        s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${cg.lhs.name} = 
          new (output_buffer->get_next(0, sizeof(Trie<${Environment.layout}, ${annotationType}>))) 
          Trie<${Environment.layout}, ${annotationType}>(${cg.lhs.attrs.length});""")
        s.println("{")
        val lastBlock = if(cg.attrs.length == 0) cg.lhs.name else cg.lhs.attrs.last
        emitTopDown(s,cg.attrs.length == 0,top_down,lastBlock,cg)
        s.println("}")
      }
      case None =>
    }
  }

}