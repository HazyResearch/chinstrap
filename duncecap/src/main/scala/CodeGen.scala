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

  def emitBuildRelation(): Unit = {
    println("PLACE BUILD RELATION CODE HERE")
  }
}