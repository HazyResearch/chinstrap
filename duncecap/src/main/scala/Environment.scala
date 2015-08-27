package DunceCap

import scala.collection.mutable

class Relation(val name:String, val types:List[String], val encodings:List[String]){
  override def equals(that: Any): Boolean =
    that match {
      case that: Relation => that.encodings.equals(encodings) && that.types.equals(types) && that.name.equals(name)
      case _ => false
    }
}
class Encoding(val name:String, val _type:String){}

object Environment {
  var dbPath:String = ""
  var numThreads = 48
  var numNUMA = 4
  var layout = "hybrid"
  var algorithm = ""
  var yanna = true // delete this crapt
  var pipeline = false

  var quiet = false

  val debug = true
  val astNodes = mutable.MutableList[ASTNode]()
  val relations = mutable.Map[String,mutable.Map[String, Relation]]()
  val encodings = mutable.Map[String, Encoding]()

  def addASTNode(node:ASTNode): Unit = {
    astNodes += node
  }

  def addRelation(name: String, r : Relation): Unit = {
    if(!relations.contains(name)){
      relations += (name -> mutable.Map(r.name -> r) )
    } else {
      //assert(!relations(name).contains(r.name))
      relations(name) += (r.name -> r)
    }
  }
  
  def addEncoding(e : Encoding): Unit = {
    if(!encodings.contains(e.name)){
      encodings += (e.name -> e)
    }
  }

  def emitASTNodes(s:CodeStringBuilder): Unit = {
    CodeGen.emitCode(s, astNodes.toList.sortBy(astN => astN.order))
  }

  def clearASTNodes(): Unit = {
    astNodes.clear()
  }
}
