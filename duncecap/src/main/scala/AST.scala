package DunceCap

abstract trait ASTStatement {
  def code(s : CodeStringBuilder)
  def updateEnvironment = {}
}

/*abstract trait AST
case class ASTProgram extends ASTStatement {
}*/

case class ASTLoadStatement(rel : ASTRelation, filename : ASTStringLiteral, format : String) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    s.println("#include <iostream>")
    s.println("#include <unordered_map>")
    s.println("#include \"emptyheaded.hpp\"")
    s.println("#include \"utils/io.hpp\"")

    s.println("extern \"C\" void run(std::unordered_map<std::string, void*>& relations) {")

    s.println(s"Relation<uint64_t,uint64_t>* ${rel.identifierName} = new Relation<uint64_t,uint64_t>();")
    assert(format == "tsv") // TODO : add in csv option
    s.println(s"""tsv_reader f_reader("data/${(filename.str)}");""")
    s.println("char *next = f_reader.tsv_get_first();")
    s.println(s" ${rel.identifierName}->num_columns = 0;")
    s.println("while(next != NULL){")
    for (i <- (0 to (rel.attrs.size-1)).toList) {
      s.println(s"${rel.identifierName}->get<${i}>()->append_from_string(next);")
      s.println("next = f_reader.tsv_get_next();")
    }
    s.println(s"${rel.identifierName}->num_columns++;")
    s.println("}")

    // make sure to add the relation and encoding we've just made to the maps the server keeps track of
    s.println(s"""relations["${rel.identifierName}"] = ${rel.identifierName};""")

    s.println(s"""std::cout << ${rel.identifierName}->num_columns << " rows loaded." << std::endl;""")
    s.println("}")
  }

  override def updateEnvironment: Unit = {
    Environment.addRelationBinding(rel.identifierName, rel.attrs.values.toList.map((optTypes : Option[String]) => optTypes.get))
  }
}

case class ASTAssignStatement(identifier : ASTIdentifier, expression : ASTExpression) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    expression.code(s)
  }
}

case class ASTPrintStatement(expression : ASTExpression) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = {
    expression match {
      case ASTScalar(identifierName) => {
        s.println("#include <iostream>")
        s.println("#include <unordered_map>")
        s.println("#include \"emptyheaded.hpp\"")
        s.println("#include \"utils/io.hpp\"")

        s.println("extern \"C\" void run(std::unordered_map<std::string, void*>& relations) {")

        val typeString = Environment.getTypes(identifierName).mkString(", ")
        s.println(s"""Relation<${typeString}> * ${identifierName}= (Relation<${typeString}> *)relations["${identifierName}"];""")
        s.println(s"""std::cout << "${identifierName} has " << ${identifierName}->num_columns << " rows loaded." << std::endl;""")
        s.println("}")
      }
      case _ => println(expression)
    }
  }
}

abstract trait ASTExpression extends ASTStatement
case class ASTCount(expression : ASTExpression) extends ASTExpression {
  override def code(s: CodeStringBuilder): Unit = ???
}

case class ASTJoinAndSelect(rels : List[ASTRelation], selectCriteria : List[ASTCriterion]) extends ASTExpression {

  def getDistinctAttrs(rels : List[ASTRelation]): List[(String, String)] = {
    val attrsAndTypes = rels.map((rel : ASTRelation) => rel.attrs.keys.toList.zipWithIndex.map(
      ( attrAndIndex : (String, Int)) => (attrAndIndex._1, Environment.getTypes(rel.identifierName)(attrAndIndex._2)))).flatten
    attrsAndTypes.toSet.toList.sorted
  }

  def emitEncodingForAttr(s: CodeStringBuilder, attr : String, attrType: String, relsAttrs : List[(String, List[String])]) = {
    /**
     * std::vector<Column<uint64_t>*> *a_attributes = new std::vector<Column<uint64_t>*>();
     * a_attributes->push_back(R_ab.get<0>());
     * a_attributes->push_back(R_ab.get<1>());
     * Encoding<uint64_t> a_encoding(a_attributes);
     */
    s.println(s"""std::vector<Column<${attrType}>*> *${attr}_attributes = new std::vector<Column<${attrType}>*>();""")
    relsAttrs.map((relAttrs : (String, List[String])) => s.println(
      s"""${attr}_attributes->push_back(${relAttrs._1}->get<${relAttrs._2.indexOf(attr)}>());""") )
    s.println(s""" Encoding<${attrType}> ${attr}_encoding(${attr}_attributes);""")
  }

  def emitTrieBuilding(s: CodeStringBuilder, allAttrs: List[String], relsAttrs : List[(String, List[String])]) = {
    /*std::vector<Column<uint32_t>*> *ER_ab = new std::vector<Column<uint32_t>*>();
    ER_ab->push_back(a_encoding.encoded->at(0)); //perform filter, selection
    ER_ab->push_back(a_encoding.encoded->at(1));

    //add some sort of lambda to do selections
    Trie *TR_ab = Trie::build(ER_ab,[&](size_t index){
      return ER_ab->at(0)->at(index) < ER_ab->at(1)->at(index);
    }); */

    // emit code to specify the levels of the tries
    relsAttrs.map((relAttrs : (String, List[String])) => s.println(
      s"""std::vector<Column<uint32_t>*> *E${relAttrs._1} = new std::vector<Column<uint32_t>*>();"""))
    allAttrs.map((attr : String) => relsAttrs.filter((relAttr : (String, List[String])) => relAttr._2.contains(attr))
      .unzip._1.zipWithIndex.map((relAttr : (String, Int)) => s.println(
      s"""E${relAttr._1}->push_back(${attr}_encoding.encoded->at(${relAttr._2}));""")))

    // emit code to construct each of the tries
    relsAttrs.unzip._1.map((identifier : String) => s.println(s"""Trie *T${identifier} = True::build(E${identifier}, [&](size_t index){});""") )
  }

  def emitAttrIntersectionBuffers(s: CodeStringBuilder, attrs : List[String]) = {
    attrs.map((attr : String) => s.println(s"""allocator::memory<uint8_t> ${attr}_buffer(10000); // TODO"""))
  }

   def emitAttrIntersection(s: CodeStringBuilder, attr : String, relsAttrs :  List[(String, List[String])]) = {
    s.println(s"""Set<uinteger> ${attr}(${attr}_buffer.get_memory(tid)); //initialize the memory""")

  }

  def emitAttrLoopOverResult() = {
  // this should include the walking down the trie, so that when you recursively call emitNPRR, you do so with different rel names
  }

  def emitNPRR(attrs : List[String], relsAttrs : List[(String, List[String])]) : Unit = {
    if (attrs.isEmpty) return

    val currAttr = attrs.head
    // emitAttrIntersection(currAttr, )
    // emitAttrLoopOverResult(currAttr, )
  }

  override def code(s: CodeStringBuilder): Unit = {
    // call emitEncodingForAttr and emitTrieBuilding here

    val relations = rels.map((rel : ASTRelation) => (rel.identifierName, rel.attrs.keys.toList.reverse))
    val attrList = getDistinctAttrs(rels)
    attrList.map(( attrAndType : (String, String)) => emitEncodingForAttr(s, attrAndType._1, attrAndType._2, relations))
    emitTrieBuilding(s, attrList.unzip._1, relations)
    emitAttrIntersectionBuffers(s, attrList.unzip._1)
    s.println("par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){")
    s.println("return a + b;")
    s.println("});")
    //emitNPRR(attrList, rels)
    s.println("// TODO nprr ")
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
