package DunceCap

import scala.collection.mutable

object Environment {
  val relationTypes = mutable.Map[String, List[String]]()
  val rewrittenRelationTypes = mutable.Map[String, List[String]]()
  val emittedEncodingPushBack = mutable.Set[(String, String, Int)]()

  def addRelationBinding(identifier : String, types : List[String]): Unit = {
    relationTypes += (identifier -> types)
  }

  def addRewrittenRelationBinding(identifier : String, types : List[String]): Unit = {
    relationTypes += (identifier -> types)
  }

  def dropRewrittenRelationBinding(): Unit = {
    rewrittenRelationTypes.empty
  }

  def getTypes(identifier : String): List[String] = {
    relationTypes.get(identifier).fold(rewrittenRelationTypes(identifier))(x => x)
  }

  def alreadyEmittedEncoding(pushback : (String, String, Int)): Boolean = {
    emittedEncodingPushBack.contains(pushback)
  }

  def addEmittedEncoding(pushback : (String, String, Int)) = {
    emittedEncodingPushBack.add(pushback)
  }

  def clearEmittedEncodings() = {
    emittedEncodingPushBack.empty
  }
}
