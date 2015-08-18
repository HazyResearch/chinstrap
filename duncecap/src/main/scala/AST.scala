package DunceCap

import scala.collection.mutable

/**
 * Explanation of types below:
 *
 * Every node in the AST is an ASTNode. Some of these are ASTStatements,
 * others are ASTExpressions.
 *
 * All ASTNodes emit code, and have an optimize method that might transform the AST rooted at that node.
 * Since ASTStatements can have side effects, these have an updateEnvironment method.
 */
abstract trait ASTNode {
  def code(s: CodeStringBuilder)
  val order = 0
}

case class ASTCreateDB() extends ASTNode {
  override val order = 1
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitInitCreateDB(s)
  }
}

case class ASTLoadRelation(sourcePath:String,rel:Relation) extends ASTNode {
  override val order = 2
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitLoadRelation(s,sourcePath,rel)
  }
}

case class ASTBuildEncodings() extends ASTNode {
  override val order = 3
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitBuildEncodings(s)
  }
}

case class ASTBuildTrie(sourcePath:String,rel:Relation) extends ASTNode {
  override val order = 4
  override def code(s: CodeStringBuilder): Unit = {
    CodeGen.emitBuildTrie(s,sourcePath,rel)
  }
}
