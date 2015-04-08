package DunceCap

import scala.collection.mutable

object Environment {
  val relations = mutable.Map[String, List[String]]()

  def addRelationBinding(identifier : String, types : List[String]): Unit = {
    relations += (identifier -> types)
  }

  def getTypes(identifier : String): List[String] = {
    relations(identifier)
  }
}
