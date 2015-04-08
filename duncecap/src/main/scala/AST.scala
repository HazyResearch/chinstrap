package DunceCap

abstract trait ASTStatement {
  def code(s : CodeStringBuilder)
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
    s.println(s"relations[\"${rel.identifierName}\"] = ${rel.identifierName};")
    s.println("}")
  }
}

case class ASTAssignStatement(identifier : ASTIdentifier, expression : ASTExpression) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = ???
}

case class ASTPrintStatement(expression : ASTExpression) extends ASTStatement {
  override def code(s: CodeStringBuilder): Unit = ???
}

abstract trait ASTExpression extends ASTStatement
case class ASTCount(expression : ASTExpression) extends ASTExpression {
  override def code(s: CodeStringBuilder): Unit = ???
}
case class ASTJoinAndSelect(rels : List[ASTRelation], selectCriteria : List[ASTCriterion]) extends ASTExpression {

  def getDistinctAttrs(rels : List[ASTRelation]): List[String] = {
    rels.map((rel : ASTRelation) => rel.attrs.keySet).foldLeft(Set[String]())(
      (acc : Set[String], newSet : Set[String]) => acc ++ newSet).toList
  }

  def emitAttrIntersectionBuffers(attrs : List[String], rels: ASTRelation) = {
    // emit buffers for each
  }

  def emitNPRR(attrs : List[String], rels: ASTRelation) : Unit = {
    if (attrs.isEmpty) return

    val currAttr = attrs.head


  }

  override def code(s: CodeStringBuilder): Unit = {

    // Get access to all the relevant tries
    rels.map((rel: ASTRelation) => s.println(s"Block* head = T${rel.identifierName}->head;"))

    // the number of buffers we allocate needs to be equal to the number of set intersections we plan to do
    val attrList = getDistinctAttrs(rels)
    //emitAttrIntersectionBuffers(attrList, rels)
    //emitNPRR(attrList, rels)

    /*    auto qt = debug::start_clock();
    head->data.par_foreach([&](size_t tid, uint32_t d1){
      Block *l2 = head->map.at(d1);
      Set<uinteger> C(buffer.get_memory(tid));
      l2->data.foreach([&](uint32_t d2){
        if(head->map.count(d2)){
          size_t count = ops::set_intersect(&C,&l2->data,&head->map.at(d2)->data)->cardinality;
          num_triangles.update(tid,count);
        }
      });
    });

    size_t result = num_triangles.evaluate(0);
    debug::stop_clock("Query",qt); */

    s.println("par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){")
    s.println("return a + b;")
    s.println("});")

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
