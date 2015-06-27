package DunceCap

import scala.collection.mutable

object Environment {
  val yanna = false
  val relationTypes = mutable.Map[String, List[String]]()

  def addRelationBinding(identifier : String, types : List[String]): Unit = {
    relationTypes += (identifier -> types)
  }

  def getTypes(identifier : String): List[String] = {
    relationTypes(identifier)
  }

}
