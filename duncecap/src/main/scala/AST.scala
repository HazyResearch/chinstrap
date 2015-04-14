package DunceCap

/**
 * All code generation should start from this object:
 */
object CodeGen {
  def emitHeaderAndCodeForAST(s: CodeStringBuilder, root: ASTNode) = {
    s.println("#define WRITE_VECTOR 1")
    s.println("#include <iostream>")
    s.println("#include <unordered_map>")
    s.println("#include \"emptyheaded.hpp\"")
    s.println("#include \"utils/io.hpp\"")

    s.println("extern \"C\" void run(std::unordered_map<std::string, void*>& relations) {")
    root.code(s)
    s.println("}")
  }
  def emitMainMethod(s : CodeStringBuilder): Unit = {
    s.println("int main() {")
    s.println("std::unordered_map<std::string, void*> relations;")
    s.println("run(relations);")
    s.println("}")
  }
}

abstract trait ASTNode {
  def code(s: CodeStringBuilder)
}

case class ASTStatements(statements : List[ASTStatement]) extends ASTNode {
  def code(s: CodeStringBuilder): Unit = {
    statements.map((statement : ASTStatement) => {
      s.println("{")
      statement.code(s)
      s.println("}")
      statement.updateEnvironment
    })

  }
}

abstract trait ASTStatement extends ASTNode {
  def updateEnvironment = {}
}

case class ASTLoadStatement(rel : ASTRelation, filename : ASTStringLiteral, format : String) extends ASTNode with ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    s.println(s"Relation<uint64_t,uint64_t>* ${rel.identifierName} = new Relation<uint64_t,uint64_t>();")
    assert(format == "tsv") // TODO : add in csv option
    s.println(s"""tsv_reader f_reader("data/${(filename.str)}");""")
    s.println("char *next = f_reader.tsv_get_first();")
    s.println(s" ${rel.identifierName}->num_columns = 0;")
    s.println("while(next != NULL){")
    for (i <- (0 to (rel.attrs.size-1)).toList) {
      s.println(s"${rel.identifierName}->get<${i}>().append_from_string(next);")
      s.println("next = f_reader.tsv_get_next();")
    }
    s.println(s"${rel.identifierName}->num_columns++;")
    s.println("}")

    // make sure to add the relation and encoding we've just made to the maps the server keeps track of
    s.println(s"""relations["${rel.identifierName}"] = ${rel.identifierName};""")

    s.println(s"""std::cout << ${rel.identifierName}->num_columns << " rows loaded." << std::endl;""")
  }

  override def updateEnvironment: Unit = {
    Environment.addRelationBinding(rel.identifierName, rel.attrs.values.toList.map((optTypes : Option[String]) => optTypes.get))
  }
}

case class ASTAssignStatement(identifier : ASTIdentifier, expression : ASTExpression) extends ASTNode with ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    expression.code(s)
  }
}

case class ASTPrintStatement(expression : ASTExpression) extends ASTNode with ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    expression match {
      case ASTScalar(identifierName) => {

        val typeString = Environment.getTypes(identifierName).mkString(", ")
        s.println(s"""Relation<${typeString}> * ${identifierName}= (Relation<${typeString}> *)relations["${identifierName}"];""")
        s.println(s"""std::cout << "${identifierName} has " << ${identifierName}->num_columns << " rows loaded." << std::endl;""")
      }
      case _ => println(expression)
    }
  }
}

abstract trait ASTExpression extends ASTNode
case class ASTCount(expression : ASTExpression) extends ASTExpression {
  override def code(s: CodeStringBuilder): Unit = ???
}

case class ASTJoinAndSelect(rels : List[ASTRelation], selectCriteria : List[ASTCriterion]) extends ASTExpression {

  def getDistinctAttrs(rels : List[ASTRelation]): List[(String, String)] = {
    val attrsAndTypes = rels.map((rel : ASTRelation) => {
      rel.attrs.keys.toList.zipWithIndex.map(( attrAndIndex : (String, Int)) => {
        (attrAndIndex._1, Environment.getTypes(rel.identifierName)(attrAndIndex._2))
      })
    }).flatten
    attrsAndTypes.toSet.toList.sorted
  }

  def emitRelationLookupAndCast(s: CodeStringBuilder, rels : List[String]) = {
    rels.map((identifierName : String) => {
      val typeString = Environment.getTypes(identifierName).mkString(", ")
      s.println(s"""Relation<${typeString}> * ${identifierName}= (Relation<${typeString}> *)relations["${identifierName}"];""")
    })
  }

  def emitEncodingForAttr(s: CodeStringBuilder, attr : String, attrType: String, relsAttrs : List[(String, List[String])]) = {
    s.println(s"""std::vector<Column<${attrType}>> *${attr}_attributes = new std::vector<Column<${attrType}>>();""")
    relsAttrs.map((relAttrs : (String, List[String])) => {
      if (relAttrs._2.contains(attr))
        s.println(s"""${attr}_attributes->push_back(${relAttrs._1}->get<${relAttrs._2.indexOf(attr)}>());""")
    })
    s.println(s""" Encoding<${attrType}> ${attr}_encoding(${attr}_attributes); // TODO heap allocate""")
  }

  def emitTrieBuilding(s: CodeStringBuilder, allAttrs: List[String], relsAttrs : List[(String, List[String])]) = {
    // emit code to specify the levels of the tries
    relsAttrs.map((relAttrs : (String, List[String])) => s.println(
      s"""std::vector<Column<uint32_t>> *E${relAttrs._1} = new std::vector<Column<uint32_t>>();"""))
    allAttrs.map((attr : String) => relsAttrs.filter((relAttr : (String, List[String])) => relAttr._2.contains(attr))
      .unzip._1.zipWithIndex.map((relAttr : (String, Int)) => s.println(
      s"""E${relAttr._1}->push_back(${attr}_encoding.encoded.at(${relAttr._2}));""")))

    // emit code to construct each of the tries
    relsAttrs.unzip._1.map((identifier : String) => s.println(s"""Trie *T${identifier} = Trie::build(E${identifier}, [&](size_t index){return true;});""") )
  }

  def emitAttrIntersectionBuffers(s: CodeStringBuilder, attrs : List[String]) = {
    attrs.map((attr : String) => s.println(s"""allocator::memory<uint8_t> ${attr}_buffer(${attr}_encoding.key_to_value.size()); // TODO"""))
  }

  def emitAttrIntersection(s: CodeStringBuilder, lastIntersection : Boolean, attr : String, relsAttrs :  List[(String, List[String])]) : List[(String, List[String])]= {
    val relsAttrsWithAttr = relsAttrs.filter(( rel : (String, List[String])) => rel._2.contains(attr))
    if (relsAttrsWithAttr.size == 1 || attr == "a") { // TODO: hack
      // no need to emit an intersection
      s.println( s"""Set<uinteger> ${attr} = ${relsAttrsWithAttr.head._1}->data;""")
    } else {
      s.println(s"""Set<uinteger> ${attr}(${attr}_buffer.get_memory(tid)); //initialize the memory""")
      // emit an intersection for the first two relations
      s.println(s"""${attr} = ops::set_intersect(&${attr},&${relsAttrsWithAttr.head._1}->data,&${relsAttrsWithAttr.tail.head._1}->data);""")
      val restOfRelsAttrsWithAttr = relsAttrsWithAttr.tail.tail
      restOfRelsAttrsWithAttr.map((rel : (String, List[String])) => s.println(s"""${attr} = ops::set_intersect(&${attr},&${attr}->data,&${rel._1}->data);"""))
    }

    if (lastIntersection) {
      s.println(s"""const size_t count = ${attr}.cardinality;""")
      s.println("num_triangles.update(tid,count);")
      List[(String, List[String])]()
    } else {
      // update relsAttrs by peeling attr off the attribute lists, and adding ->map.at(attr_i) to the relation name
      relsAttrs.map((rel : (String, List[String])) => {
        if (rel._2.head.contains(attr)) {
          (rel._1 + s"->map.at(${attr}_i)", rel._2.tail)
        } else {
          rel
        }
      })
    }


  }

  def emitAttrLoopOverResult(s: CodeStringBuilder, outermostLoop : Boolean, attrs : List[String], relsAttrs : List[(String, List[String])]) = {
    // this should include the walking down the trie, so that when you recursively call emitNPRR, you do so with different rel names
    if (outermostLoop) {
      s.println(s"""${attrs.head}.par_foreach([&](size_t tid, uint32_t ${attrs.head}_i){""")
      emitNPRR(s, false, attrs.tail, relsAttrs)
      s.println("});")
    } else {
      s.println(s"""${attrs.head}.foreach([&](uint32_t ${attrs.head}_i) {""")
      emitNPRR(s, false, attrs.tail, relsAttrs)
      s.println("});")
    }
  }

  def emitNPRR(s: CodeStringBuilder, initialCall : Boolean, attrs : List[String], relsAttrs : List[(String, List[String])]) : Unit = {
    if (attrs.isEmpty) return

    val currAttr = attrs.head
    if (attrs.tail.isEmpty) {
      emitAttrIntersection(s, true, currAttr, relsAttrs)
    } else {
      val updatedRelsAttrs = emitAttrIntersection(s, false, currAttr, relsAttrs)
      emitAttrLoopOverResult(s, initialCall, attrs, updatedRelsAttrs)
    }

  }

  override def code(s: CodeStringBuilder): Unit = {
    val relations = rels.map((rel : ASTRelation) => (rel.identifierName, rel.attrs.keys.toList.reverse))
    val attrList = getDistinctAttrs(rels)
    emitRelationLookupAndCast(s, relations.unzip._1)
    attrList.map(( attrAndType : (String, String)) => emitEncodingForAttr(s, attrAndType._1, attrAndType._2, relations))
    emitTrieBuilding(s, attrList.unzip._1, relations)
    emitAttrIntersectionBuffers(s, attrList.unzip._1)
    s.println("par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){")
    s.println("return a + b;")
    s.println("});")
    val firstBlockOfTrie = relations.map(( rel: (String, List[String])) => ("T" + rel._1 + "->head", rel._2))
    emitNPRR(s, true, attrList.unzip._1, firstBlockOfTrie)
    s.println("size_t result = num_triangles.evaluate(0);")
    s.println("std::cout << result << std::endl;")
  }
}
case class ASTStringLiteral(str : String) extends ASTExpression {
  override def code(s: CodeStringBuilder): Unit = ???
}

abstract trait ASTCriterion extends ASTExpression
case class ASTEq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???
}
case class ASTLeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???
}
case class ASTGeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???
}
case class ASTLess(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???
}
case class ASTGreater(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???
}
case class ASTNeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion {
  override def code(s: CodeStringBuilder): Unit = ???
}

abstract trait ASTIdentifier extends ASTExpression
case class ASTRelation(identifierName : String, attrs : Map[String, Option[String]]) extends ASTIdentifier {
  override def code(s: CodeStringBuilder): Unit = ???
} // attribute name to option with type, or no type if it can be inferred
case class ASTScalar(identifierName : String) extends ASTIdentifier {
  override def code(s: CodeStringBuilder): Unit = ???
}
