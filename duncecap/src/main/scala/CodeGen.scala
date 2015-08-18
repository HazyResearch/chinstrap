package DunceCap

import scala.collection.mutable
/**
 * All code generation methods start from this object:
 */
object CodeGen {
  var layout = "hybrid"
  
  def emitCode(s: CodeStringBuilder, root: List[ASTNode]) = {
    emitHeader(s,root)
    emitMainMethod(s)
  }

  def emitHeader(s:CodeStringBuilder, root:List[ASTNode]) = {
    s.println("#define GENERATED")
    s.println("#include \"main.hpp\"")
    s.println(s"""extern \"C\" long run(std::unordered_map<std::string, void*>& relations, std::unordered_map<std::string, Trie<${layout}>*> tries, std::unordered_map<std::string, std::vector<void*>*> encodings) {""")
    s.println(s"""long query_result = -1;""")
    root.foreach{_.code(s)}
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
      s.println(s"""Encoding<${encoding._type}>* ${encoding.name}_encoding = new Encoding<${encoding._type}>();""")
    })
    s.print("\n")
  }

  def emitLoadRelation(s: CodeStringBuilder,sourcePath:String,rel:Relation): Unit = {
    s.println("""////////////////////emitLoadRelation////////////////////""")
    s.println("{")
    s.println("""auto start_time = debug::start_clock();""")
    val codeString = s"""tsv_reader f_reader("${sourcePath}");
                         char *next = f_reader.tsv_get_first();
                         while(next != NULL){ """
    s.println(codeString)
    for (i <- 0 until rel.attrs.length){
      s.println(s"""${rel.encoding(i)}_encodingMap->update(${rel.name}->append_from_string<${i}>(next));""") 
      s.println(s"""next = f_reader.tsv_get_next();""")
    }
    s.println(s"""${rel.name}->num_rows++; }""")
    s.println(s"""debug::stop_clock("READING RELATION ${rel.name}",start_time);""")
    s.println("}")
    s.print("\n")
  }

  def emitBuildEncodings(s: CodeStringBuilder): Unit = {
    s.println("""////////////////////emitBuildEncodings////////////////////""")
    s.println("{")
    s.println("""auto start_time = debug::start_clock();""")
    Environment.encodings.foreach(tuple => {
      val (name,encoding) = tuple
      s.println(s"""${name}_encoding->build(${name}_encodingMap);""")
      s.println(s"""delete ${name}_encodingMap;""")
    })
    s.println(s"""debug::stop_clock("BUILDING ENCODINGS",start_time);""")
    s.println("} \n")
  }

  def emitBuildTrie(s: CodeStringBuilder,sourcePath:String,rel:Relation): Unit = {
    s.println("""////////////////////emitBuildTrie////////////////////""")
    s.println("{")
    s.println("""auto start_time = debug::start_clock();""")
    s.println("} \n")
  }

}