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

  def emitTrieBlock(s:CodeStringBuilder,attr:String,accessor:Accessor,seenAccessors:Set[String]): Unit = {
    if(!seenAccessors.contains(accessor.getName())){
      s.print(s"""const TrieBlock<${Environment.layout},${annotationType}>* ${accessor.getName()} = """)
      val getBlock = if(accessor.level == 0) 
          s"""Trie_${accessor.trieName}->head;""" 
        else 
          s"""${accessor.getPrevName()}->get_block(${accessor.getPrevAttr()}_d);"""
      s.println(getBlock)
    }
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

  def emitAnnotationInitialization(s:CodeStringBuilder,
    cga:CodeGenNPRRAttr,
    tid:String,
    expression:String,
    scalarResult:Boolean,
    aggregates:List[String]): Unit = {
    val extraAnnotations = cga.accessors.map(acc => acc.annotation.isDefined).reduce( (a,b) => {a || b})
    cga.agg match {
      case Some(aggregate) => {
        if(cga.last && !extraAnnotations){ //always the last attribute?
          val name = if(aggregates.indexOf(cga.attr) == 0) "annotation" else s"""annotation_${cga.attr}"""
          val exp2 = if(aggregates.indexOf(cga.attr) == 0) "(" else s"""(annotation_tmp * """
          s.println(s"""${annotationType} ${name} = ${exp2} (${expression}*${cga.attr}.cardinality));""")
        }else {
          if(scalarResult && aggregates.indexOf(cga.attr) == 0) emitAggregateReducer(s,cga)
          else if(aggregates.indexOf(cga.attr) == 0) s.println(s"""${annotationType} annotation = (${annotationType})0;""")    
          else s.println(s"""${annotationType} annotation_${cga.attr} = (${annotationType})0;""")            
        }
      } case None => 
    }
  }

  def emitMaterializedSelection(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String,selection:SelectionCondition,loopSet:String) : Unit = {
    s.println(s"""//emitMaterializedSelection""")
    val setName = if(cga.materializeViaSelectionsBelow) s"""${cga.attr}_filtered""" else cga.attr

    s.println(s"""size_t ${cga.attr}_s_i = 0;""")
    s.println(s"""uint32_t* ${cga.attr}_data = (uint32_t*)output_buffer->get_next(${tid},${loopSet}.number_of_bytes);""")
    s.println(s"""${loopSet}.foreach([&](uint32_t ${cga.attr}_s_d){""")
    val condition = if(selection.condition == "=") "==" else selection.condition
    s.println(s"""if(${cga.attr}_s_d ${condition} ${cga.attr}_selection){""")
    s.println(s"""${cga.attr}_data[${cga.attr}_s_i++] = ${cga.attr}_s_d;""")
    s.println(s"""}""")
    s.println(s"""});""")
    s.println(s"""const size_t ${cga.attr}_data_range = (${cga.attr}_s_i > 0) ? (${cga.attr}_data[${cga.attr}_s_i-1]-${cga.attr}_data[0]) : 0;""")
    s.println(s"""Set<${Environment.layout}> ${setName}((uint8_t*)${cga.attr}_data,${cga.attr}_s_i,${cga.attr}_data_range,${cga.attr}_s_i*sizeof(uint32_t),type::UINTEGER);""")
  }

  def emitSetupFilteredSet(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String) : Unit = {
    if(cga.first)
      s.println(s"""std::atomic<size_t> filter_index(0);""")
    else 
      s.println(s"""size_t filter_index = 0;""")

    s.println(s"""uint32_t* filtered_data = (uint32_t*)output_buffer->get_next(${tid},${cga.attr}_filtered.cardinality*sizeof(uint32_t));""")
  }

    //the materialized set condition could be nested so we want a GOTO to break out once a single cond is met
    //to do this will only work with one nested condition.
  def emitCheckConditions(s:CodeStringBuilder,selection:SelectionCondition,attr:String,loopSet:String,first:Boolean) : Unit = {
    s.println(s"""${loopSet}.foreach_until([&](uint32_t ${attr}_s_d){""")
    val condition = if(selection.condition == "=") "==" else selection.condition
    s.println(s"""ret_value = (${attr}_s_d ${condition} ${attr}_selection);""")
    val fi = if(first) "filter_index.fetch_add(1)" else "filter_index++"
    s.println(s"""if(ret_value) filtered_data[${fi}] = filter_value;""")
    s.println(s"""return ret_value;""")
    s.println(s"""});""")
  }

  def emitNewSet(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String,materializeWithSelectionsBelowInParallel:Boolean): Unit = {
    val attr = cga.attr
    val accessors = cga.accessors
    val setName = if(cga.materializeViaSelectionsBelow) s"""${attr}_filtered""" else attr
    val bufferName = if(cga.materializeViaSelectionsBelow) "tmp_buffer" else "output_buffer"
    val otherBufferName = if(cga.materializeViaSelectionsBelow) "output_buffer" else "tmp_buffer" //trick to save mem when
    //materializations below occur

    if(accessors.length == 1){
      (cga.selection,cga.materialize,cga.last) match {
        case (Some(selection),true,_) => emitMaterializedSelection(s,cga,tid,selection,s"""${accessors.head.getName()}->set""")
        case (Some(selection),false,true) => emitCheckConditions(s,selection,cga.attr,s"""${accessors.head.getName()}->set""",materializeWithSelectionsBelowInParallel)
        case (_,_,_) => s.println(s"""Set<${Environment.layout}> ${setName} = ${accessors.head.getName()}->set;""")
      }
    } else if(accessors.length == 2){
      s.println(s"""Set<${Environment.layout}> ${setName}(${bufferName}->get_next(${tid},alloc_size_${attr}));""")
      emitIntersection(s,setName,accessors.head.getName()+"->set",accessors.last.getName()+"->set",cga.selection,cga.attr)
    } else{
      s.println(s"""Set<${Environment.layout}> ${setName}(${bufferName}->get_next(${tid},alloc_size_${attr}));""")
      s.println(s"""Set<${Environment.layout}> ${attr}_tmp(${otherBufferName}->get_next(${tid},alloc_size_${attr})); //initialize the memory""")
      var tmp = (accessors.length % 2) == 1
      var name = if(tmp) s"""${attr}_tmp""" else setName
      emitIntersection(s,name,accessors(0).getName()+"->set",accessors(1).getName()+"->set",cga.selection,cga.attr)
      (2 until accessors.length).foreach(i => {
        tmp = !tmp
        name = if(tmp) s"""${attr}_tmp""" else setName
        val opName = if(!tmp) s"""${attr}_tmp""" else setName
        emitIntersection(s,name,opName,accessors(i).getName()+"->set",None,cga.attr)
      })
      s.println(s"""${otherBufferName}->roll_back(${tid},alloc_size_${attr});""")
    }
  }

  def emitRollBack(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String):Unit = {
    val attr = cga.attr
    val accessors = cga.accessors
    if(accessors.length != 1){
      if(cga.materializeViaSelectionsBelow){
        s.println(s"""tmp_buffer->roll_back(${tid},alloc_size_${attr}-${attr}_filtered.number_of_bytes);""")
      } else{
        s.println(s"""output_buffer->roll_back(${tid},alloc_size_${attr}-${attr}.number_of_bytes);""")
      }
    }
    if(cga.materializeViaSelectionsBelow) emitSetupFilteredSet(s,cga,tid) //must occur after roll back
  }

  def emitNewTrieBlock(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String,annotatedAttr:Option[String]): Unit = {
    s.println("//emitNewTrieBlock")
    s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* TrieBlock_${cga.attr} = 
      new(output_buffer->get_next(${tid}, sizeof(TrieBlock<${Environment.layout},${annotationType}>))) 
      TrieBlock<${Environment.layout},${annotationType}>(${cga.attr});""")
    annotatedAttr match {
      case Some(a) => {
        if(a == cga.attr && !cga.last){
          s.println(s"""TrieBlock_${cga.attr}->init_pointers_and_data(${tid},output_buffer);""")
        } else if(a == cga.attr){
          s.println(s"""TrieBlock_${cga.attr}->value = 1; //hack fix constant value""")
        } else if(!cga.last){
          s.println(s"""TrieBlock_${cga.attr}->init_pointers(${tid},output_buffer);""")
        }
      }
      case None =>
        if(!cga.last)
          s.println(s"""TrieBlock_${cga.attr}->init_pointers(${tid},output_buffer);""")
    }
  }

  def emitSetTrieBlock(s:CodeStringBuilder,cga:CodeGenNPRRAttr,lhs:String,setData:Boolean): Unit = {
    s.println("//emitSetTrieBlock")
    if(cga.materialize){
      cga.prev match {
        case Some(a) =>
          s.println(s"""TrieBlock_${a}->set_block(${a}_i,${a}_d,TrieBlock_${cga.attr});""")
        case None =>
          s.println(s"""Trie_${lhs}->head = TrieBlock_${cga.attr};""")
      }
    }
    if(setData){
      cga.prev match {
        case Some(a) =>
          s.println(s"""TrieBlock_${a}->set_data(${a}_i,${a}_d,annotation);""")
        case None =>
          s.println(s"""//shouldn't happen this is just a reducer;""")
      }
    }
  }

  def emitSetComputations(s:CodeStringBuilder,cga:CodeGenNPRRAttr,
    seenAccessors:Set[String],tid:String,materializeWithSelectionsBelowInParallel:Boolean): Unit = {
    s.println("//emitSetComputations")
    cga.accessors.foreach(accessor => {emitTrieBlock(s,cga.attr,accessor,seenAccessors)})
    emitMaxSetAlloc(s,cga.attr,cga.accessors)
    emitNewSet(s,cga,tid,materializeWithSelectionsBelowInParallel)
    emitRollBack(s,cga,tid)
  }

  def emitAggregateReducer(s:CodeStringBuilder,cga:CodeGenNPRRAttr):Unit = {
    s.println("//emitAggregateReducer")
    cga.agg match {
      case Some("SUM") => 
        s.println(s"""par::reducer<${annotationType}> annotation(0,[](size_t a, size_t b){ return a + b; });""")
      case _ =>
    }
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

  def emitForEach(s:CodeStringBuilder,cga:CodeGenNPRRAttr,expression:String) : Unit = {
    val attr = cga.attr
    val first = cga.first
    val last = cga.last
    s.println("""//emitForEach""")
    (cga.agg,cga.materialize,cga.materializeViaSelectionsBelow) match {
      case (Some(_),_,_) | (None,false,_) | (None,true,true) =>{  //this should only be for constant expressions really otherwise we need a foreach_index
        val setName = if(cga.materializeViaSelectionsBelow) attr + "_filtered" else attr
        val noMaterializeCheckSelection = (cga.accessors.length == 1 && !cga.materialize && !cga.last && cga.selection.isDefined)
        val until = if(noMaterializeCheckSelection) "_until" else ""
        if(first){
          s.println(s"""${setName}.par_foreach${until}([&](size_t tid, uint32_t ${attr}_d){ (void) tid;""")
        } else{
          s.println(s"""${setName}.foreach${until}([&](uint32_t ${attr}_d) {""")
        }
        if(cga.materializeViaSelectionsBelow){ 
          s.println(s"""const uint32_t filter_value = ${attr}_d;""")
          s.println(s"""bool ret_value = false;""")
        }
        if(noMaterializeCheckSelection) emitNoMaterializeCheckSelection(s,cga)
      }
      case (None,true,_) => {
        if(first){
          s.println(s"""${attr}.par_foreach_index([&](size_t tid, uint32_t ${attr}_i, uint32_t ${attr}_d){ (void) tid;""")
        } else{
          s.println(s"""${attr}.foreach_index([&](uint32_t ${attr}_i, uint32_t ${attr}_d) {""")
        }
      }
    }
  }

  def emitRewriteOutputTrie(s:CodeStringBuilder,outName:String,prevName:String,scalarResult:Boolean) : Unit = {
    s.println("//emitRewriteOutputTrie")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${outName} = Trie_${prevName};""")
  }

  def emitAnnotationTemporary(s:CodeStringBuilder, cga:CodeGenNPRRAttr, aggregates:List[String], expression:String) : Unit = {
    cga.agg match {
      case Some(a) => {
        if(aggregates.indexOf(cga.attr) != (aggregates.length-1) ){
          val extraAnnotations = cga.accessors.filter(acc => acc.annotation.isDefined)
          if(extraAnnotations.length != 0){
            s.println(s"""${annotationType} annotation_tmp = ${extraAnnotations.head.getName()}->get_data(${cga.attr}_d);""")
          } else if(aggregates.indexOf(cga.attr) == 0){
            s.println(s"""${annotationType} annotation_tmp = ${expression};""")
          } else {
            s.println(s"""annotation_tmp *= ${expression};""")
          }
        }
      }
      case None =>
    }
  }

  def emitAnnotationComputation(s:CodeStringBuilder,cg:CodeGenGHD,cga:CodeGenNPRRAttr,agg:String,tid:String,ea:Boolean):Unit = {
    s.println("//emitAnnotationComputation")
    if(cga.last)
      s.println(s"""output_buffer->roll_back(${tid},${cga.attr}.number_of_bytes);""")
    val index = if(ea) cg.aggregates.indexOf(cga.attr)+1 else cg.aggregates.indexOf(cga.attr)
    val extraAnnotation = cga.accessors.filter(acc => acc.annotation.isDefined)
    if(index == 1 && cg.scalarResult){
      if(ea)
        s.println(s"""annotation.update(tid,annotation_tmp);""")
      else
        s.println(s"""annotation.update(${tid},annotation_${cga.attr});""")
    } else if(index != 0){
      val name = if(index == 1) "annotation" else s"""annotation_${cg.aggregates(index-1)}"""
      agg match {
        case "SUM" => {
          if(extraAnnotation.length == 0)
            s.println(s"""${name} += annotation_${cga.attr};""")
          else 
            s.println(s"""annotation_${cga.attr} += ${extraAnnotation.head.getName()}->get_data(${cga.attr}_d);""")
        }
        case _ => s.println(s"""//${agg} not implemented""")
      }
    } else {
      cga.prev match {
        case Some(a) =>
          s.println(s"""TrieBlock_${a}->set_data(${a}_i,${a}_d,annotation);""")
        case None => //scalar result
          s.println(s"""Trie_${cg.lhs.name}->annotation = annotation.evaluate(0);""")
      }
    }
  }

  //the materialized set condition could be nested so we want a GOTO to break out once a single cond is met
  def emitBuildNewSet(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String) : Unit = {
    s.println(s"""//emitBuildNewSet""")
    val fi = if(cga.first) "filter_index.load()" else "filter_index"
    if(cga.first){
      s.println(s"""tbb::parallel_sort(filtered_data,filtered_data+${fi});""");
    }
    s.println(s"""const size_t ${cga.attr}_range = (${fi} > 0) ? (filtered_data[${fi}-1]-filtered_data[0]) : 0;""")
    s.println(s"""Set<${Environment.layout}> ${cga.attr}((uint8_t*)filtered_data,${fi},${cga.attr}_range,${fi}*sizeof(uint32_t),type::UINTEGER);""")
    s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* TrieBlock_${cga.attr} = 
      new(output_buffer->get_next(${tid}, sizeof(TrieBlock<${Environment.layout},${annotationType}>))) 
      TrieBlock<${Environment.layout},${annotationType}>(${cga.attr});""")

    if(!cga.first)
      s.println(s"""TrieBlock_${cga.attr}->init_pointers(${tid},output_buffer);""")
  }

  def emitStartQueryTimer(s:CodeStringBuilder) : Unit = {
    s.println("""auto query_time = debug::start_clock();""")
  }
  def emitStopQueryTimer(s:CodeStringBuilder) : Unit = {
    s.println("""debug::stop_clock("QUERY TIME",query_time);""")
  }

  def emitSelectionValues(s:CodeStringBuilder, cg:CodeGenGHD) : Unit = {
    //emit the selection values first if they exist
    cg.attrs.foreach(cga => {
      cga.selection match{
        case Some(selection) =>{
          val (encoding,atype) = cg.attrToEncoding(cga.attr)
          s.println(s"""const uint32_t ${cga.attr}_selection = Encoding_${encoding}->value_to_key.at(${selection.value});""")
        }
        case None =>
      }
    })
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

  def emitNPRR(s: CodeStringBuilder,name: String,cg:CodeGenGHD,noWork:Option[String],td:Option[List[CodeGenTopDown]]): Unit = {
    s.println("""////////////////////emitNPRR////////////////////""")

    if(Environment.debug){
      println("---------debug----------")
      println("name: " + name)
      println("no work: " + noWork)
      println("lhs: " + cg.lhs.name + " " + cg.lhs.attrs)
      println("expression: " + cg.expression)
      println("aggregates: " + cg.aggregates)
      println("annotated attribute: " + cg.annotatedAttr)
      println("attr to encoding map: " + cg.attrToEncoding)
      println("scalar result: " + cg.scalarResult)
      println("code gen attr")
      if(td.isDefined){
        println("top down: ")
        td.get.foreach(t =>{
          println("attr: " + t.attr)
          t.accessors.foreach(acc => {
            println("\taccessors: " + acc.trieName + " " + acc.level + " " + acc.attrs)
          })
        })
      }
      else
        println("top down: " + td)
      cg.attrs.foreach(cga => {
        println()
        println("attr: " + cga.attr)
        println("agg: " + cga.agg)
        cga.accessors.foreach(acc => {
          println("\taccessors: " + acc.trieName + " " + acc.level + " " + acc.attrs + " " + acc.annotation)
        })
        if(cga.selection.isDefined)
          println("selection: " + cga.selection.get.condition + " " + cga.selection.get.value)
        else 
          println("selection: " + cga.selection)
        println("materialize: " + cga.materialize)
        println("first: " + cga.first)
        println("last: " + cga.last)
        println("prev: " + cga.prev)
        println("materializeViaSelectionsBelow: " + cga.materializeViaSelectionsBelow)
        println("checkSelectionNotMaterialize: " + cga.checkSelectionNotMaterialize)
      })
      println("---------end debug----------")
    }



    if(noWork.isDefined){
      s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${cg.lhs.name} = Trie_${noWork.get};""")
      return
    }

    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${cg.lhs.name} = 
      new (output_buffer->get_next(0, sizeof(Trie<${Environment.layout}, ${annotationType}>))) 
      Trie<${Environment.layout}, ${annotationType}>(${cg.lhs.attrs.length});""")
    s.println("{")
    //if(!Environment.quiet) 
    s.println("""auto start_time = debug::start_clock();""")
    s.println("//")
    
    emitSelectionValues(s,cg)

    //should probably be recursive
    var materializeWithSelectionsBelowInParallel = false
    var seenAccessors = Set[String]()
    val extraAnnotations = cg.attrs.last.accessors.map(acc => acc.annotation.isDefined).reduce( (a,b) => {a || b})
    cg.attrs.foreach(cga => {
      //TODO: Refactor all of these into the CGA class there is no reason they can't be computed ahead of time.
      val tid = if(cga.first) "0" else "tid"
      materializeWithSelectionsBelowInParallel ||= (cga.first&&cga.materializeViaSelectionsBelow)
      emitSetComputations(s,cga,seenAccessors,tid,materializeWithSelectionsBelowInParallel)
      if(cga.materialize && !cga.materializeViaSelectionsBelow) emitNewTrieBlock(s,cga,tid,cg.annotatedAttr)
      emitAnnotationInitialization(s,cga,tid,cg.expression,cg.scalarResult,cg.aggregates)
      if(!cga.last || extraAnnotations) emitForEach(s,cga,cg.expression)
      emitAnnotationTemporary(s,cga,cg.aggregates,cg.expression)
      seenAccessors ++= cga.accessors.map(_.getName())
    })

    //emit top down code if you have it
    td match {
      case Some(top_down) =>
        val lastBlock = if(cg.attrs.length == 0) cg.lhs.name else cg.lhs.attrs.last
        emitTopDown(s,cg.attrs.length == 0,top_down,lastBlock,cg)
      case None =>
    }

    //close out an emit materializations if necessary
    cg.attrs.reverse.foreach(cga =>{
      val cgaExtraAnnotations = cga.accessors.map(acc => acc.annotation.isDefined).reduce( (a,b) => {a || b})
      val tid = if(cga.first) "0" else "tid"
      cga.agg match {
        case Some(a) => emitAnnotationComputation(s,cg,cga,a,tid,extraAnnotations)
        case None => {
          if(cga.materializeViaSelectionsBelow) emitBuildNewSet(s,cga,tid)
          val setData = if(cga.prev.isDefined && cg.annotatedAttr.isDefined) cga.prev == cg.annotatedAttr.get else false 
          emitSetTrieBlock(s,cga,cg.lhs.name,setData)
        }
      }
      if(!cga.first) emitNoMaterializeCheckSelectionEnd(s,cg.attrs(cg.attrs.indexOf(cga)-1))
      if(!cga.first || extraAnnotations) s.println("});")
      if(!cga.first && cgaExtraAnnotations) s.println(s"""annotation_tmp *= annotation_${cga.attr};""")
    })
    if(extraAnnotations)
      s.println(s"""Trie_${cg.lhs.name}->annotation = annotation.evaluate(0);""")

    //if(!Environment.quiet) 
    s.println(s"""debug::stop_clock("Bag ${name}",start_time);""")
    s.println("} \n")
  }

}