package DunceCap

/**
 * This file contains classes for AST nodes
 * that might exist after we rewrite the AST a little to reflect the query plan
 */

case class ASTTwoBagJoinAndSelect(bag1: ASTJoinAndSelect, bag2: ASTJoinAndSelect) extends ASTExpression {
  override def code(s: CodeStringBuilder): Unit = {
    // emit NPRR for bag1, and put the counts in an array
    bag1.code(s)
    // emitNPRR for bag2, and mult by the counts in that array when giving values to the reducer
    bag2.code(s)
    // maybe should add passing in of some function to ASTJoinAndSelect (this might be used to materializing as well)
  }

  override def optimize: Unit = { /* do nothing */ }
}

object ASTOptimizer {
  def optimizeAST(root: ASTNode) = {
    root.optimize
  }
}


