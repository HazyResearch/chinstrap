package DunceCap

/**
 * All code generation should start from this object:
 */
object CodeGen {
  def emitHeaderAndCodeForAST(s: CodeStringBuilder, root: ASTNode) = {
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
  val layout = "hybrid"
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
    val relationTypes = rel.attrs.values.toList.map((optTypes : Option[String]) => optTypes.get).mkString(",")
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
        s.println(s"""std::cout << "${identifierName} has " << ${identifierName}->num_rows << " rows loaded." << std::endl;""")
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

  import NPRRSetupUtil._

  private def emitRelationLookupAndCast(s: CodeStringBuilder, rels : List[String]) = {
    rels.map((identifierName : String) => {
      val typeString = Environment.getTypes(identifierName).mkString(", ")
      s.println(s"""Relation<${typeString}> * ${identifierName}= (Relation<${typeString}> *)relations["${identifierName}"];""")
    })
  }

  private def emitAttrIntersectionBuffers(s: CodeStringBuilder, attrs : List[String], relations : Relations, equivalanceClasses : List[EquivalenceClass]) = {
    attrs.map((attr : String) => {
      val encoding : Column= getEncodingRelevantToAttr(attr, relations, equivalanceClasses)
      s.println(s"""allocator::memory<uint8_t> ${attr}_buffer(${mkEncodingName(encoding)}_encoding.key_to_value.size());""")
    })
  }

  private def emitAttrIntersection(s: CodeStringBuilder, lastIntersection : Boolean, attr : String, relsAttrs :  List[(String, List[String])]) : List[(String, List[String])]= {
    val relsAttrsWithAttr = relsAttrs.filter(( rel : (String, List[String])) => rel._2.contains(attr)).unzip._1.distinct
    assert(!relsAttrsWithAttr.isEmpty)

    if (relsAttrsWithAttr.size == 1) { // TODO: no need to intersect same col in repeated relation
      // no need to emit an intersection
      s.println( s"""Set<${layout}> ${attr} = ${relsAttrsWithAttr.head}->data;""")
     } else {
      s.println(s"""Set<${layout}> ${attr}(${attr}_buffer.get_memory(tid)); //initialize the memory""")
      // emit an intersection for the first two relations
      s.println(s"""${attr} = ops::set_intersect(&${attr},&${relsAttrsWithAttr.head}->data,&${relsAttrsWithAttr.tail.head}->data);""")
      val restOfRelsAttrsWithAttr = relsAttrsWithAttr.tail.tail
      restOfRelsAttrsWithAttr.map((rel : String) => s.println(s"""${attr} = ops::set_intersect(&${attr},&${attr},&${rel}->data);"""))
    }

    if (lastIntersection) {
      s.println(s"""const size_t count = ${attr}.cardinality;""")
      s.println("num_triangles.update(tid,count);")
      List[(String, List[String])]()
    } else {
      // update relsAttrs by peeling attr off the attribute lists, and adding ->map.at(attr_i) to the relation name
      relsAttrs.map((rel : (String, List[String])) => {
        if (!rel._2.isEmpty && rel._2.head.contains(attr)) {
          (rel._1 + s"->get_block(${attr}_i)", rel._2.tail)
        } else {
          rel
        }
      })
    }
  }

  private def emitAttrLoopOverResult(s: CodeStringBuilder, outermostLoop : Boolean, attrs : List[String], relsAttrs : List[(String, List[String])]) = {
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

  private def emitNPRR(s: CodeStringBuilder, initialCall : Boolean, attrs : List[String], relsAttrs : List[(String, List[String])]) : Unit = {
    if (attrs.isEmpty) return

    val currAttr = attrs.head
    if (attrs.tail.isEmpty) {
      emitAttrIntersection(s, true, currAttr, relsAttrs)
    } else {
      val updatedRelsAttrs = emitAttrIntersection(s, false, currAttr, relsAttrs)
      emitAttrLoopOverResult(s, initialCall, attrs, updatedRelsAttrs)
    }
  }

  private def emitAttrEncoding(s : CodeStringBuilder, rewrittenAttrName : String, relName: String, attrIndex: Int): Unit = {
    s.println( s"""${rewrittenAttrName}_attributes->push_back(${relName}->get<${attrIndex}>()); """)
  }

  private def emitEncodingInitForAttr(s: CodeStringBuilder, attr : String, attrType: String) = {
    s.println(s"""std::vector<Column<${attrType}>> *${attr}_attributes = new std::vector<Column<${attrType}>>();""")
  }

  private def emitBuildEncodingForAttr(s: CodeStringBuilder,  attr : String, attrType: String) = {
    s.println(s"""Encoding<${attrType}> ${attr}_encoding(${attr}_attributes); // TODO heap allocate""")
  }

  private def emitEncodingForEquivalenceClass(s : CodeStringBuilder, klass : EquivalenceClass) ={
    val encodingName = klass.head._1 + "_" + klass.head._2
    val encodingType = Environment.getTypes(klass.head._1)(klass.head._2)
    emitEncodingInitForAttr(s, encodingName, encodingType)
    klass.map({ case (rName, index) => {
      emitAttrEncoding(s, encodingName, rName, index)
    }})
    emitBuildEncodingForAttr(s, encodingName, encodingType)
  }

  def emitTrieBuilding(s: CodeStringBuilder, relsAttrs : RWRelations, equivalenceClasses: List[EquivalenceClass]) = {
    // emit code to specify the levels of the tries
    relsAttrs.map((relAttrs : RWRelation) => {
      s.println(s"""std::vector<Column<uint32_t>> *E${relAttrs.name} = new std::vector<Column<uint32_t>>();""")
      relAttrs.attrs.map((index : Int) => {
        val equivClass = equivalenceClasses.find((klass : EquivalenceClass) => klass.contains((relAttrs.name, index)))
        assert(equivClass.isDefined)
        val equivClassName = equivClass.get.head._1 + "_" + equivClass.get.head._2
        val encodingIndex = equivClass.get.indexOf((relAttrs.name, index))
        s.println(s"""E${relAttrs.name}->push_back(${equivClassName}_encoding.encoded.at(${encodingIndex}));""")
      })
    })

    // emit code to construct each of the tries
    relsAttrs.map((relAttrs : RWRelation) => relAttrs.name).map((identifier : String) => s.println(s"""Trie<${layout}> *T${identifier} = Trie<${layout}>::build(E${identifier}, [&](size_t index){return true;});""") )
  }

  override def code(s: CodeStringBuilder): Unit = {
    /**
     * relations is a list of tuples, where the first element is the name, and the second is the list of attributes
     */
    val relations = rels.map((rel : ASTRelation) => new Relation(rel.attrs.keys.toList.reverse, rel.identifierName))

    /**
     * We get a distinct list of them so we can look them up and have a pointer to them
     */
    val distinctRewrittenRelations = getDistinctRelations(relations)
    emitRelationLookupAndCast(s, distinctRewrittenRelations.map((rewrittenRelation : RWRelation) => rewrittenRelation.name))

    /**
     * Now emit the encodings of each of the attrs
     */
    val equivalenceClasses = buildEncodingEquivalenceClasses(relations)
    equivalenceClasses.map((klass : EquivalenceClass) => {
      emitEncodingForEquivalenceClass(s, klass)
    })

    /**
     * emit the trie building
     */
    emitTrieBuilding(s, distinctRewrittenRelations, equivalenceClasses)

    /**
     * Emit the buffers that we will do intersections for each attr in
     */
    val fullOriginalAttrList = relations.map((rel : Relation) => rel.attrs).flatten.distinct.sorted
    emitAttrIntersectionBuffers(s, fullOriginalAttrList, relations, equivalenceClasses)

    s.println("par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){")
    s.println("return a + b;")
    s.println("});")
    s.println("int tid = 0;")
    val firstBlockOfTrie = relations.map(( rel: Relation) => ("T" + rel.name + "->head", rel.attrs))
    emitNPRR(s, true, fullOriginalAttrList, firstBlockOfTrie)
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
