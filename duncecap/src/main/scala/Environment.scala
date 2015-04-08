package DunceCap

import scala.collection.mutable


object Environment {
  // mapping from name of relation in repl to name of relation in C++
  private var identifierNames : mutable.Map[String, String] = mutable.Map[String, String]()
  private var identifierID = 0

  def bindEmittedIdentifierName(DCIdentifierName : String): Unit = {

  }

}
