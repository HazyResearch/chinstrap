package DunceCap

class Expression(inputExpression:String) {
  val (lhs,rhs) = parseExpression(inputExpression)

  println("LHS: " + lhs + " RHS: " + rhs)

  val (rel,attr) = parseLHS(lhs)

  println(rel + " " + attr)

  def parseExpression(line:String) : (String,String) = {
    val rexpr = """^(.*)<-(.*)""".r
    line match {
      case rexpr(lhs,rhs) => (lhs,rhs)
      case _ => ("FUCK","FUCK")
    }
  }
  def parseLHS(line:String) : (String,String) = {
    val rexpr = """^(\w+)\((.*)\).*""".r
    line match {
      case rexpr(r,a) => (r,a)
      case _ => ("FUCK","FUCK")
    } 
  }
}
