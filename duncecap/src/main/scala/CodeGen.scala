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

  def emitWriteBinaryTrie(s: CodeStringBuilder,rel:Relation): Unit = {
    s.println("""////////////////////emitWriteBinaryTrie////////////////////""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println("//buildTrie")
    s.println(s"""Trie_${rel.name}->to_binary("${Environment.dbPath}/relations/${rel.name}/");""")
    if(!Environment.quiet) s.println(s"""debug::stop_clock("WRITING BINARY TRIE ${rel.name}",start_time);""")
    s.println("} \n")
  }

  def emitWriteBinaryEncoding(s: CodeStringBuilder,enc:Encoding): Unit = {
    s.println("""////////////////////emitWriteBinaryEncoding////////////////////""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println("//buildTrie")
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

  def emitPrintRelation(s: CodeStringBuilder,rel:QueryRelation): Unit = {
    s.println("""////////////////////emitWriteBinaryEncoding////////////////////""")
    s.println("{")
    //load from binary first
    val qr = rel.name + "_" + (0 until rel.attrs.size).toList.mkString("")
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

  def emitNPRR(s: CodeStringBuilder,name: String,cg:CodeGenGHD): Unit = {
    s.println("""////////////////////emitNPRR////////////////////""")
    s.println(s"""Trie<${Environment.layout},${annotationType}>* ${cg.lhs.name} = NULL;""")
    s.println("{")
    if(!Environment.quiet) s.println("""auto start_time = debug::start_clock();""")
    s.println("//")
    cg.attrs.foreach(cga => {

    })
    if(!Environment.quiet) s.println(s"""debug::stop_clock("Bag ${name}",start_time);""")
    s.println("} \n")
  }

}