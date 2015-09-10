package DunceCap

import scala.collection.immutable.List
import scala.collection.immutable.ListMap
import scala.util.parsing.combinator.RegexParsers

class AggregateExpression(val op:String,
  val attrs:List[String],
  val init:String){

}
class AnnotationExpression( val boundVariable:String,
  val expr1:String,
  val agg:AggregateExpression,
  val expr2:String) {

}

object DCParser extends RegexParsers {
  def run(line:String,s:CodeStringBuilder) : Unit = {
    this.parseAll(this.statements, line) match {
      case DCParser.Success(ast, _) => {
        Environment.emitASTNodes(s)
      }
    }
  }

  //some basic expressions
  def identifierName:Parser[String] = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  def selectionElement:Parser[String] = """"[^"]*"|\d+""".r
  def aggOp:Parser[String] = """SUM""".r
  def selectionOp:Parser[String] = """=""".r
  def typename:Parser[String] = """long|string|float""".r
  def emptyStatement = "".r ^^ {case r => List()}
  def emptyString = "".r ^^ {case r => ""}
  def emptyStatementMap:Parser[Map[String,String]] = "".r ^^ {case r => Map[String,String]()}

  //for the lhs expression
  def lhsStatement = relationIdentifier | scalarIdentifier
  def relationIdentifier: Parser[QueryRelation] = identifierName ~ (("(" ~> attrList) <~ ")") ^^ {case id~attrs => new QueryRelation(id, attrs)}
  def scalarIdentifier: Parser[QueryRelation] = identifierName ~ (("(" ~> emptyStatement) <~ ")") ^^ {case a~l => new QueryRelation(a,l)}
  def attrList : Parser[List[String]] = notLastAttr | lastAttr
  def notLastAttr = identifierName ~ ("," ~> attrList) ^^ {case a~rest => a +: rest}
  def lastAttr = identifierName ^^ {case a => List(a)}
  
  //for the join query
  def joinStatement:Parser[List[SelectionRelation]] = multipleJoinIdentifiers | singleJoinIdentifier 
  def multipleJoinIdentifiers = (singleJoinIdentifier <~ ",") ~ joinStatement ^^ {case t~rest => t ++: rest}
  def singleJoinIdentifier = identifierName ~ (("(" ~> joinAttrList) <~ ")") ^^ {case id~attrs => List( new SelectionRelation(id, attrs) )}
  def joinAttrList : Parser[List[(String,String,String)]] = notLastJoinAttr | lastJoinAttr
  def notLastJoinAttr = selectionStatement ~ ("," ~> joinAttrList) ^^ {case a~rest => a +: rest}
  def lastJoinAttr = selectionStatement ^^ {case a => List(a)} 
  def selectionStatement : Parser[(String,String,String)] =  selection | emptySelection
  def selection: Parser[(String,String,String)] = (identifierName ~ selectionOp ~ selectionElement) ^^ {case a~b~c => (a,b,c) }
  def emptySelection: Parser[(String,String,String)] = identifierName ^^ {case a => (a,"","")}


  //returns the bound annotation mapped to the expression with a annotation in the middle
  //the annotation is (operation,attrs,init)
  def emptyAggregate:Parser[AggregateExpression] = "".r ^^ {case r => new AggregateExpression("",List(),"")}
  def emptyAnnotationMap:Parser[AnnotationExpression] = emptyAggregate ^^ {case r => new AnnotationExpression("","",r,"")}
  def annotationStatement:Parser[AnnotationExpression] = annotation | emptyAnnotationMap
  def expression:Parser[String] = """[^\.|SUM]*""".r
  def annotation:Parser[AnnotationExpression] = (identifierName <~ "=") ~ expression ~ aggregateStatement ~ expression ^^ {case a~b~c~d => new AnnotationExpression(a,b,c,d)} 
  def aggInit:Parser[String] = (";" ~> selectionElement <~ ")") | emptyString ^^ { case a => a}
  def aggregateStatement = (aggOp ~ ("(" ~> attrList) ~ aggInit) ^^ {case a~b~c => new AggregateExpression(a,b,c) }

  def joinType:Parser[String] = """+""".r
  def emptyJoinType = "".r ^^ {case r => """*"""}
  def joinTypeStatement:Parser[String] =  (("join" ~> "=" ~> joinType) | emptyJoinType) ^^ {case a => a}
  def joinAndAnnotationStatement = joinStatement ~ annotationStatement ^^ { case a~b => 
    println(a)
    println(b)
  }

  //def queryStatement = (lhsStatement ~ (":-" ~> aggregateStatement) ~ joinStatement ~ (aggregateExpressionStatement <~ ".") ) ^^ {case a~b~c~d => Environment.addASTNode(new ASTQueryStatement(a,b,c,d))}
  def queryStatement = (lhsStatement ~ (":-" ~> joinTypeStatement) ~ joinAndAnnotationStatement <~ ".")
  def lambdaExpression = ( (identifierName <~ ":-") ~ ("(" ~> lhsStatement <~ "=") ~ ("{" ~ joinAndAnnotationStatement ~ "}") )
  def printStatement = (lhsStatement <~ ".") ^^ {case l => Environment.addASTNode(new ASTPrintStatement(l))}
  def statement = queryStatement | lambdaExpression | printStatement
  def statements = rep(statement)
}
