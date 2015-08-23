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

  def emitEncodeRelation(s: CodeStringBuilder,rel:Relation,name:String): Unit = {
    s.println("""////////////////////emitEncodeRelation////////////////////""")
    s.println(s"""std::vector<std::vector<uint32_t>>* Encoded_${rel.name} = new std::vector<std::vector<uint32_t>>();""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println("//encodeRelation")
    for (i <- 0 until rel.types.length){
      s.println(s"""Encoded_${rel.name}->push_back(*Encoding_${rel.encodings(i)}->encode_column(&${name}->get<${i}>()));""")
    }
    if(!Environment.quiet) s.println(s"""debug::stop_clock("ENCODING ${rel.name}",start_time);""")
    s.println("} \n")
  }

  def emitBuildTrie(s: CodeStringBuilder,rel:Relation): Unit = {
    s.println("""////////////////////emitBuildTrie////////////////////""")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${rel.name} = NULL;""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println("//buildTrie")
    s.println(s"""Trie_${rel.name} = Trie<${Environment.layout},${annotationType}>::build(Encoded_${rel.name},[&](size_t index){ (void) index; return true;});""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("BUILDING TRIE ${rel.name}",start_time);""")
    s.println("} \n")
  }

  def emitWriteBinaryTrie(s: CodeStringBuilder,relName:String): Unit = {
    s.println("""////////////////////emitWriteBinaryTrie////////////////////""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println(s"""Trie_${relName}->to_binary("${Environment.dbPath}/relations/${relName}/");""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("WRITING BINARY TRIE ${relName}",start_time);""")
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
  def emitLoadBinaryEncoding(s: CodeStringBuilder,encodings:List[String]): Unit = {
    s.println("""////////////////////emitLoadBinaryEncoding////////////////////""")
    if(!Environment.quiet) s.println("""auto belt = debug::start_clock();""")
    encodings.foreach(e => {
      s.println(s"""Encoding<${Environment.encodings(e)._type}>* Encoding_${e} = Encoding<${Environment.encodings(e)._type}>::from_binary("${Environment.dbPath}/encodings/${e}/");""")
      s.println(s"""(void) Encoding_${e};""")
    })
    if(!Environment.quiet) s.println(s"""debug::stop_clock("LOADING ENCODINGS",belt);""")
    s.print("\n")
  }
  def emitLoadBinaryRelation(s: CodeStringBuilder,relations:List[String]): Unit = {
    s.println("""////////////////////emitLoadBinaryEncoding////////////////////""")
    if(!Environment.quiet) s.println("""auto btlt = debug::start_clock();""")
    relations.foreach(qr => {
      s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${qr} = Trie<${Environment.layout},${annotationType}>::from_binary("${Environment.dbPath}/relations/${qr}/");""")
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
    s.println("""////////////////////emitWriteBinaryEncoding////////////////////""")
    s.println("{")
    //load from binary first
    val qr = rel.name + "_" + (0 until rel.attrs.size).toList.mkString("_")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${qr} = Trie<${Environment.layout},${annotationType}>::from_binary("${Environment.dbPath}/relations/${qr}/");""")
    val loadEncodings = (0 until rel.attrs.size).map(i => { Environment.relations(rel.name)(qr).encodings(i)}).toList.distinct
    emitLoadBinaryEncoding(s,loadEncodings)

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

  def emitIntersection(s:CodeStringBuilder,resultSet:String,op1:String,op2:String) : Unit = {
    s.println(s"""${resultSet} = *ops::set_intersect(&${resultSet},
      (const Set<${Environment.layout}> *)&${op1}->set,
      (const Set<${Environment.layout}> *)&${op2}->set);""")
  }

  def emitAnnotationInitialization(s:CodeStringBuilder,
    cga:CodeGenNPRRAttr,
    tid:String,
    expression:String,
    scalarResult:Boolean,
    aggregates:List[String]): Unit = {
    cga.agg match {
      case Some(aggregate) => {
        if(cga.last){ //always the last attribute?
          s.println(s"""${annotationType} annotation_${cga.attr} = (annotation_tmp * (${expression}*${cga.attr}.cardinality));""")
        }else {
          if(scalarResult && aggregates.indexOf(cga.attr) == 0) emitAggregateReducer(s,cga)
          else if(aggregates.indexOf(cga.attr) == 0) s.println(s"""${annotationType} annotation = (${annotationType})0;""")    
          else s.println(s"""${annotationType} annotation_${cga.attr} = (${annotationType})0;""")            
        }
      } case None => 
    }
  }

  def emitNewSet(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String): Unit = {
    val attr = cga.attr
    val accessors = cga.accessors
    if(accessors.length == 1){
      s.println(s"""Set<${Environment.layout}> ${attr} = ${accessors.head.getName()}->set;""")
    } else if(accessors.length == 2){
      s.println(s"""Set<${Environment.layout}> ${attr}(output_buffer->get_next(${tid},alloc_size_${attr}));""")
      emitIntersection(s,attr,accessors.head.getName(),accessors.last.getName())
    } else{
      s.println(s"""Set<${Environment.layout}> ${attr}(output_buffer->get_next(${tid},alloc_size_${attr}));""")
      s.println(s"""Set<${Environment.layout}> ${attr}_tmp(tmp_buffer->get_next(${tid},alloc_size_${attr})); //initialize the memory""")
      var tmp = (accessors.length % 2) == 1
      var name = if(tmp) s"""${attr}_tmp""" else s"""${attr}"""
      emitIntersection(s,name,accessors(0).getName(),accessors(1).getName())
      (2 until accessors.length).foreach(i => {
        tmp = !tmp
        name = if(tmp) s"""${attr}_tmp""" else s"""${attr}"""
        val opName = if(!tmp) s"""${attr}_tmp""" else s"""${attr}"""
        emitIntersection(s,name,opName,accessors(i).getName())
      })
      s.println(s"""tmp_buffer->roll_back(${tid},alloc_size_${attr});""")
    }
  }

  def emitRollBack(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String):Unit = {
    val attr = cga.attr
    val accessors = cga.accessors
    if(accessors.length != 1)
      s.println(s"""output_buffer->roll_back(${tid},alloc_size_${attr}-${attr}.number_of_bytes);""")
  }

  def emitNewTrieBlock(s:CodeStringBuilder,cga:CodeGenNPRRAttr,tid:String,annotatedAttr:Option[String]): Unit = {
    s.println("//emitNewTrieBlock")
    s.println(s"""TrieBlock<${Environment.layout},${annotationType}>* TrieBlock_${cga.attr} = 
      new(output_buffer->get_next(${tid}, sizeof(TrieBlock<${Environment.layout},${annotationType}>))) 
      TrieBlock<${Environment.layout},${annotationType}>(${cga.attr});""")
    annotatedAttr match {
      case Some(a) => {
        if(a == cga.attr)
          s.println(s"""TrieBlock_${cga.attr}->init_pointers_and_data(${tid},output_buffer);""")
        else 
          s.println(s"""TrieBlock_${cga.attr}->init_pointers(${tid},output_buffer);""")
      }
      case None =>
        s.println(s"""TrieBlock_${cga.attr}->init_pointers(${tid},output_buffer);""")
    }
  }

  def emitSetTrieBlock(s:CodeStringBuilder,cga:CodeGenNPRRAttr,lhs:String): Unit = {
    s.println("//emitSetTrieBlock")
    cga.prev match {
      case Some(a) =>
        s.println(s"""TrieBlock_${a}->set_block(${a}_i,${a}_d,TrieBlock_${cga.attr});""")
      case None =>
        s.println(s"""Trie_${lhs}->head = TrieBlock_${cga.attr};""")
    }
  }

  def emitSetComputations(s:CodeStringBuilder,cga:CodeGenNPRRAttr,seenAccessors:Set[String],tid:String): Unit = {
    s.println("//emitSetComputations")
    cga.accessors.foreach(accessor => {emitTrieBlock(s,cga.attr,accessor,seenAccessors)})
    emitMaxSetAlloc(s,cga.attr,cga.accessors)
    emitNewSet(s,cga,tid)
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

  def emitForEach(s:CodeStringBuilder,cga:CodeGenNPRRAttr,expression:String) : Unit = {
    val attr = cga.attr
    val first = cga.first
    val last = cga.last
    s.println("""//emitForEach""")
    cga.agg match {
      case Some(agg) =>{  //this should only be for constant expressions really otherwise we need a foreach_index
        if(first){
          s.println(s"""${attr}.par_foreach([&](size_t tid, uint32_t ${attr}_d){""")
        } else{
          s.println(s"""${attr}.foreach([&](uint32_t ${attr}_d) {""")
        }
      }
      case None => {
        if(first){
          s.println(s"""${attr}.par_foreach_index([&](size_t tid, uint32_t ${attr}_i, uint32_t ${attr}_d){""")
        } else{
          s.println(s"""${attr}.foreach_index([&](uint32_t ${attr}_i, uint32_t ${attr}_d) {""")
        }
      }
    }
  }

  def emitRewriteOutputTrie(s:CodeStringBuilder,outName:String,prevName:String,scalarResult:Boolean) : Unit = {
    s.println("//emitRewriteOutputTrie")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${outName} = Trie_${prevName};""")
    if(scalarResult){
      s.println(s"""std::cout << "Query Result: " << Trie_${outName}->annotation << std::endl;""")
    } 
  }

  def emitAnnotationTemporary(s:CodeStringBuilder, cga:CodeGenNPRRAttr, aggregates:List[String], expression:String) : Unit = {
    cga.agg match {
      case Some(a) => {
        if(aggregates.indexOf(cga.attr) == 0){
          s.println(s"""${annotationType} annotation_tmp = ${expression};""")
        } else if(aggregates.indexOf(cga.attr) != (aggregates.length-1)){
          s.println(s"""annotation_tmp *= ${expression};""")
        }
      }
      case None =>
    }
  }

  def emitAnnotationComputation(s:CodeStringBuilder,cg:CodeGenGHD,cga:CodeGenNPRRAttr,agg:String,tid:String):Unit = {
    s.println("//emitAnnotationComputation")
    s.println(s"""output_buffer->roll_back(${tid},${cga.attr}.number_of_bytes);""")
    val index = cg.aggregates.indexOf(cga.attr)
    if(index == 1 && cg.scalarResult){
      s.println(s"""annotation.update(${tid},annotation_${cga.attr});""")
    } else if(index != 0){
      val name = if(index == 1) "annotation" else s"""annotation_${cg.aggregates(index-1)}"""
      agg match {
        case "SUM" => {
          s.println(s"""${name} += annotation_${cga.attr};""")
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

  def emitNPRR(s: CodeStringBuilder,name: String,cg:CodeGenGHD): Unit = {
    s.println("""////////////////////emitNPRR////////////////////""")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* Trie_${cg.lhs.name} = 
      new (output_buffer->get_next(0, sizeof(Trie<${Environment.layout}, ${annotationType}>))) 
      Trie<${Environment.layout}, ${annotationType}>(${cg.lhs.attrs.length});""")
    s.println("{")
    //if(!Environment.quiet) 
    s.println("""auto start_time = debug::start_clock();""")
    s.println("//")
    val firstAttr = cg.attrs.head.attr
    val lastAttr = cg.attrs.last.attr
    var seenAccessors = Set[String]()
    
    //should probably be recursive
    cg.attrs.foreach(cga => {
      //TODO: Refactor all of these into the CGA class there is no reason they can't be computed ahead of time.
      val tid = if(cga.first) "0" else "tid"
      emitSetComputations(s,cga,seenAccessors,tid)
      if(cga.materialize) emitNewTrieBlock(s,cga,tid,cg.annotatedAttr)
      emitAnnotationInitialization(s,cga,tid,cg.expression,cg.scalarResult,cg.aggregates)
      if(!cga.last) emitForEach(s,cga,cg.expression)
      emitAnnotationTemporary(s,cga,cg.aggregates,cg.expression)
      seenAccessors ++= cga.accessors.map(_.getName())
    })
    //close out an emit materializations if necessary
    cg.attrs.reverse.foreach(cga =>{
      val tid = if(cga.first) "0" else "tid"
      cga.agg match {
        case Some(a) => emitAnnotationComputation(s,cg,cga,a,tid)
        case None => if(cga.materialize) emitSetTrieBlock(s,cga,cg.lhs.name)
      }
      if(!cga.first) s.println("});")
    })

    //if(!Environment.quiet) 
    s.println(s"""debug::stop_clock("Bag ${name}",start_time);""")
    s.println("} \n")
  }

}