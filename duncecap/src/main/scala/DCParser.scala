package DunceCap

import scala.collection.immutable.List
import scala.collection.immutable.ListMap
import scala.util.parsing.combinator.RegexParsers

class AggregateExpression(val op:String,
  val attrs:List[String],
  val init:String){

  def printData() = {
    println("op: " + op)
    println("attrs: " + attrs)
    println("init: " + init)
  }
}
class AnnotationExpression( val boundVariable:String,
  val expr:String,
  val agg:AggregateExpression) {

  def printData() = {
    println("Bound Variable: " + boundVariable + " expr: " + expr)
    println("agg: ")
    agg.printData()
  }
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
  def converganceCriteria:Parser[String] = """i|c""".r
  def converganceOp:Parser[String] = """=|<=|<|>|>=""".r
  def converganceCondition:Parser[String] = """\d+\.?\d*""".r 

  def selectionOp:Parser[String] = """=""".r
  def typename:Parser[String] = """long|string|float""".r
  def emptyStatement = "".r ^^ {case r => List()}
  def emptyString = "".r ^^ {case r => ""}
  def emptyStatementMap:Parser[Map[String,String]] = "".r ^^ {case r => Map[String,String]()}

  //for the lhs expression
  def lhsStatement = relationIdentifier 
  def relationIdentifier: Parser[QueryRelation] = identifierName ~ ("(" ~> attrList) ~ (aggStatement <~ ")")  ^^ {case id~attrs~(agg~t) => new QueryRelation(id, attrs, agg, t)}
  def aggStatement = ((";" ~> identifierName) ~ (":" ~> typename)) | (emptyString~emptyString)
  def attrList : Parser[List[String]] = notLastAttr | lastAttr | emptyStatement
  def notLastAttr = identifierName ~ ("," ~> attrList) ^^ {case a~rest => a +: rest}
  def lastAttr = identifierName ^^ {case a => List(a)}
  
  //for the join query
  def joinAndRecursionStatements = ((joinStatement <~ ",") | emptyStatement) ~ recursionStatement ^^ {case a~b => (a,b)}
  def recursionStatement = transitiveClosure | recursion | emptyRecursion
  def transitiveClosure = ("[" ~> joinStatement ~ emptyString <~ "]*") ~ emptyString ~ emptyString ~ emptyString ^^ {case a~b~c~d~e => (a,b,c,d,e)}
  def recursion = ("[" ~> emptyStatement) ~  identifierName ~ ("]*" ~>  converganceStatement) ^^ {case a~b~(c~d~e) => (a,b,c,d,e)}
  def converganceStatement = ( ("""{""" ~> converganceCriteria) ~ converganceOp ~ (converganceCondition <~ """}""") ) | (emptyString~emptyString~emptyString)
  def emptyRecursion = emptyStatement ~ emptyString ~ emptyString ~ emptyString ~ emptyString ^^ {case a~b~c~d~e => (a,b,c,d,e)}
  
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
  def emptyAnnotationMap:Parser[AnnotationExpression] = emptyAggregate ^^ {case r => new AnnotationExpression("","",r)}
  def annotationStatement:Parser[AnnotationExpression] = annotation | emptyAnnotationMap
  
  def expression:Parser[String] = """[^<]*""".r
  def annotation:Parser[AnnotationExpression] = (";" ~> identifierName <~ "=") ~ expression ~ aggregateStatement ^^ {case a~b~c => new AnnotationExpression(a,b,c)} 
  def aggInit:Parser[String] = (";" ~> selectionElement) | emptyString ^^ { case a => a}
  def aggOp:Parser[String] = """SUM|COUNT""".r
  def aggregateStatement = "<" ~> aggOp ~ ("(" ~> attrList) ~ (aggInit <~ ")") <~ ">" ^^ {case a~b~c => new AggregateExpression(a,b,c) }

  def joinType:Parser[String] = """\+""".r
  def emptyJoinType = "".r ^^ {case r => """*"""}
  def joinTypeStatement:Parser[String] =  (("join" ~> "=" ~> joinType <~ ";") | emptyJoinType) ^^ {case a => a}
  def joinAndAnnotationStatement = joinAndRecursionStatements ~ annotationStatement ^^ { case a~b => 
    val joins = a._1
    val recursion = a._2
    val transitiveClosure = recursion._1
    val recursiveFunction = recursion._2
    val convIdentifier = recursion._3
    val convOperator = recursion._4
    val convCriteria = recursion._5
    //a._1 contains the raw join information
    //a._2 contains the recursion information
    println("joins")
    a._1.foreach{_.printData()}
    println("recursion joins")
    a._2._1.foreach{_.printData()}
    println("recursion function: " + a._2._1) 
    println("Convergance" + convIdentifier + " " + convOperator + " " + convCriteria)
    b.printData() 

    //build a recursion object
    //do things to take COUNT(*) to SUM over all attrs init with a value of 1
  }

  def queryStatement = (lhsStatement ~ (":-" ~> joinTypeStatement) ~  joinAndAnnotationStatement <~ ".")
  def lambdaExpression = ( (identifierName <~ ":-") ~ ("(" ~> lhsStatement <~ "=>") ~ ("{" ~> joinAndAnnotationStatement <~ "}).") )
  def printStatement = (lhsStatement <~ ".") ^^ {case l => Environment.addASTNode(new ASTPrintStatement(l))}
  def statement = queryStatement | lambdaExpression | printStatement
  def statements = rep(statement)
}
