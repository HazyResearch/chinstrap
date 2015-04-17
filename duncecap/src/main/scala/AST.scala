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
    val relationTypes = rel.attrs.values.toList.map((optTypes : Option[String]) => optTypes.get).mkString(",")
    s.println(s"Relation<${relationTypes}>* ${rel.identifierName} = new Relation<${relationTypes}>();")
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

object NPRRSetupHelper {
}

case class ASTJoinAndSelect(rels : List[ASTRelation], selectCriteria : List[ASTCriterion]) extends ASTExpression {

  def emitRelationLookupAndCast(s: CodeStringBuilder, rels : List[String]) = {
    rels.map((identifierName : String) => {
      val typeString = Environment.getTypes(identifierName).mkString(", ")
      s.println(s"""Relation<${typeString}> * ${identifierName}= (Relation<${typeString}> *)relations["${identifierName}"];""")
    })
  }

  def emitTrieBuilding(s: CodeStringBuilder, relsAttrs : List[(String, List[String])], encodingHistory : Map[String, List[(String, String)]]) = {
    // emit code to specify the levels of the tries
    relsAttrs.map((relAttrs : (String, List[String])) => {
      s.println(s"""std::vector<Column<uint32_t>> *E${relAttrs._1} = new std::vector<Column<uint32_t>>();""")
      relAttrs._2.zipWithIndex.map((origAndRewrittenAttr : (String, Int)) => {
        val encodingIndex = encodingHistory(relAttrs._1 + "_" + origAndRewrittenAttr._2.toString).indexOf((relAttrs._1, origAndRewrittenAttr._1))
        s.println(s"""E${relAttrs._1}->push_back(${relAttrs._1}_${origAndRewrittenAttr._2}_encoding.encoded.at(${encodingIndex}));""")
      })
    })

    // emit code to construct each of the tries
    relsAttrs.unzip._1.map((identifier : String) => s.println(s"""Trie *T${identifier} = Trie::build(E${identifier}, [&](size_t index){return true;});""") )
  }

  def emitAttrIntersectionBuffers(s: CodeStringBuilder, attrs : List[String]) = {
    attrs.map((attr : String) => s.println(s"""allocator::memory<uint8_t> ${attr}_buffer(10000); // TODO"""))
  }

  def emitAttrIntersection(s: CodeStringBuilder, lastIntersection : Boolean, attr : String, relsAttrs :  List[(String, List[String])]) : List[(String, List[String])]= {
    val relsAttrsWithAttr = relsAttrs.filter(( rel : (String, List[String])) => rel._2.contains(attr)).unzip._1.distinct
    assert(!relsAttrsWithAttr.isEmpty)

    if (relsAttrsWithAttr.size == 1) { // TODO: no need to intersect same col in repeated relation
      // no need to emit an intersection
      s.println( s"""Set<uinteger> ${attr} = ${relsAttrsWithAttr.head}->data;""")
     } else {
      s.println(s"""Set<uinteger> ${attr}(${attr}_buffer.get_memory(tid)); //initialize the memory""")
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

  type Relation = (String, List[String])
  type RRelation = (String, List[Int])
  type Relations = List[Relation]
  type RRelations = List[RRelation]
  type RenamedRelationPair = ((String, Int),(String, String))
  type RenamedRelationInfo = List[RenamedRelationPair]

  /**
   *
   * @param relsAttrs
   * @return A list of rewritten relations, and a map from (R,0) -> List[(R,a), (R,b), etc.]
   */
  def rewriteNamesOfDuplicatedRelations(relsAttrs : List[(String, List[String])]): (RRelations, Map[(String, Int),List[(String, String)]]) = {
    val sortedRelsAttrs = relsAttrs.sortBy((relAttr : (String, List[String])) => relAttr._1)
    val distinctRelNames = relsAttrs.unzip._1.distinct
    val result = distinctRelNames.foldLeft((List[RRelation](), List[((String, Int),(String, String))]()))(
      (rewrittenRelsAttrsAndNamingMap : (RRelations, RenamedRelationInfo), relationName : String) => {
        val relsToRewrite : Relations = relsAttrs.filter((rel : Relation) => rel._1 == relationName)
        assert(!relsToRewrite.isEmpty)
        val numAttrs = relsToRewrite.head._2.size
        val renamedRelationInfo : RenamedRelationInfo  = relsToRewrite.map((rel : Relation) => {
            rel._2.zipWithIndex.map({case (attrName, index) => {
              ((relationName, index), (relationName, attrName))
            }
          })
        }).flatten

        val rewrittenAttrNames = (0 until numAttrs).toList
        val updatedRenamedRelationInfo : RenamedRelationInfo = renamedRelationInfo:::rewrittenRelsAttrsAndNamingMap._2
        ((relationName, rewrittenAttrNames)::rewrittenRelsAttrsAndNamingMap._1, updatedRenamedRelationInfo)
      })
    (result._1, result._2.groupBy((k_partialV_pair : ((String, Int),(String, String))) => k_partialV_pair._1).mapValues((v : List[RenamedRelationPair]) => v.unzip._2))
  }

  def getDistinctRelations(relations : Relations): (Relations, Relations) = {
    val sorted = relations.sortBy((relation : Relation) => relation._1)
    println(sorted)
    assert(!sorted.isEmpty)
    val relationsGroupedByName = sorted.groupBy((relation : Relation) => relation._1).toList


    val firstsOfEachName : Relations = relationsGroupedByName.map((rels : (String, List[Relation])) => {rels._2.head})
    val restOfEachName : Relations =  relationsGroupedByName.map((rels : (String, List[Relation])) => {rels._2.tail}).flatten
    println(firstsOfEachName)
    println(restOfEachName)
    return (firstsOfEachName, restOfEachName)
  }


  def emitAttrEncoding(s : CodeStringBuilder, rewrittenAttrName : String, relName: String, attrIndex: Int): Unit = {
    if (!Environment.alreadyEmittedEncoding(rewrittenAttrName, relName, attrIndex)) {
      s.println( s"""${rewrittenAttrName}_attributes->push_back(${relName}->get<${attrIndex}>()); """)
      Environment.addEmittedEncoding(rewrittenAttrName, relName, attrIndex)
    }
  }

  def emitEncodingInitForAttr(s: CodeStringBuilder, attr : String, attrType: String) = {
    s.println(s"""std::vector<Column<${attrType}>> *${attr}_attributes = new std::vector<Column<${attrType}>>();""")
  }

  def emitBuildEncodingForAttr(s: CodeStringBuilder,  attr : String, attrType: String) = {
    s.println(s"""Encoding<${attrType}> ${attr}_encoding(${attr}_attributes); // TODO heap allocate""")
  }

  def createMapAttrToRelAndIndex(attrList : List[String], mapToOriginalAttrs : Map[(String, Int),List[(String, String)]]) : Map[String, List[(String, Int)]] = {
    val attrToRelAndIndex = mapToOriginalAttrs.toList
    attrList.map((attr : String) => {
      (attr, attrToRelAndIndex.filter((elem : ((String, Int),List[(String, String)])) => elem._2.unzip._2.contains(attr)).unzip._1)
    }).toMap
  }
  override def code(s: CodeStringBuilder): Unit = {
    /**
     * relations is a list of tuples, where the first element is the name, and the second is the list of attributes
     */
    val relations = rels.map((rel : ASTRelation) => (rel.identifierName, rel.attrs.keys.toList.reverse))
    val fullAttrList = relations.map((rel : Relation) => rel._2).flatten.distinct.sorted

    /**
     * We get a distinct list of them so we can look them up and have a pointer to them
     */
    val (distinctRelations, leftOverRelations) = getDistinctRelations(relations)
    println(distinctRelations)
    emitRelationLookupAndCast(s, distinctRelations.unzip._1)

    /**
     * Now emit the encodings of each of the attrs
     */
    val (rewrittenRelations, mapToOriginalAttrs) = rewriteNamesOfDuplicatedRelations(relations)
    rewrittenRelations.map((rel : RRelation) => {
      (rel._2 zip Environment.getTypes(rel._1)).map(( attrIndexAndType : (Int, String)) => {
        emitEncodingInitForAttr(s, rel._1 + "_" + attrIndexAndType._1.toString, attrIndexAndType._2)
      })
    })


    val mapAttrToRelAndIndex : Map[String, List[(String, Int)]] = createMapAttrToRelAndIndex(fullAttrList, mapToOriginalAttrs)
    println("blah " + mapAttrToRelAndIndex)
    val history : Map[String, List[(String, String)]] = rewrittenRelations.map((rel : RRelation) => {
      rel._2.map((attrIndex : Int) => {
        val historyForIndex = mapToOriginalAttrs((rel._1, attrIndex)).unzip._2.map((originalAttrName : String) => {
          mapAttrToRelAndIndex(originalAttrName).map((occurrence : (String, Int)) => {
            emitAttrEncoding(s, rel._1 + "_" + attrIndex.toString, occurrence._1, occurrence._2)
            println((occurrence._1, occurrence._2))
            (occurrence._1, originalAttrName) /*these will give us a history of the order in which we did the push_backs*/
          })
        }).flatten
        (rel._1 + "_" + attrIndex.toString, historyForIndex)
      })
    }).flatten.toMap

    println("history")
    println(history)
    rewrittenRelations.map((rel : RRelation) => {
      (rel._2 zip Environment.getTypes(rel._1)).map(( attrIndexAndType : (Int, String)) => {
        emitBuildEncodingForAttr(s, rel._1 + "_" + attrIndexAndType._1.toString, attrIndexAndType._2)
      })
    })

    /**
     * emit the trie building
     */
    emitTrieBuilding(s, distinctRelations, history)

    /**
     * Emit the buffers that we will do intersections for each attr in
     */
    val fullOriginalAttrList = relations.map((rel : Relation) => rel._2).flatten.distinct.sorted
    emitAttrIntersectionBuffers(s, fullOriginalAttrList)

    s.println("par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){")
    s.println("return a + b;")
    s.println("});")
    s.println("int tid = 0;")
    val firstBlockOfTrie = relations.map(( rel: (String, List[String])) => ("T" + rel._1 + "->head", rel._2))
    emitNPRR(s, true, fullOriginalAttrList, firstBlockOfTrie)
    s.println("size_t result = num_triangles.evaluate(0);")
    s.println("std::cout << result << std::endl;")

    Environment.clearEmittedEncodings()
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
