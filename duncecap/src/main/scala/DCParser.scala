package DunceCap

import scala.collection.immutable.List
import scala.collection.immutable.ListMap
import scala.util.parsing.combinator.RegexParsers

object DCParser extends RegexParsers {
  
  def identifierName:Parser[String] = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  def typename:Parser[String] = """uint64_t|string|float""".r // TODO: should I wrap this in another type or just let it be a string?
  def format = """csv|tsv""".r

  def typedRelationIdentifier = identifierName ~ (("(" ~> typedAttrList) <~ ")") ^^ {case id~attrs => ASTRelation(id, attrs)}
  def typedAttrList : Parser[List[String]] = notLastTypedAttr | lastTypedAttr
  def notLastTypedAttr = typename ~ ("," ~> typedAttrList) ^^ {case t~rest => t +: rest}
  def lastTypedAttr = typename ^^ {case t => List(t)}

  def identifier = relationIdentifier | scalarIdentifier

  def relationIdentifier = identifierName ~ (("(" ~> attrList) <~ ")") ^^ {case id~attrs => ASTRelation(id, attrs)}
  def attrList : Parser[List[String]] = notLastAttr | lastAttr
  def notLastAttr = identifierName ~ ("," ~> attrList) ^^ {case a~rest => a +: rest}
  def lastAttr = identifierName ^^ {case a => List(a)}

  def scalarIdentifier = identifierName ^^ { case id => ASTScalar(id)}

  /**
   * strings may contain quotes, which must be escaped
   * any other character escaped is just itself (i.e., \c is the same as c)
   * except for \t (tab) and \n (newline)
   *
   * unescaped newlines will not be allowed by the REPL
   * (but this restriction isn't implemented here, since the repl will assume unescaped newline means end of statement)
   */
  def tab : Parser[String] = """\\t""".r ^^ {case _ => "\t"}
  def newline : Parser[String] = """\\n""".r ^^ {case _ => "\n"}
  def escapedChar : Parser[String] = "\\" ~> """.""".r ^^ {case c => c}
  def notEscaped : Parser[String] = """[^\"]""".r
  def validStringContents : Parser[String] = notEndOfString | endOfString
  def notEndOfString  : Parser[String] = ((tab|newline|escapedChar|notEscaped) ~ validStringContents) ^^ {case c~rest => c+rest}
  def endOfString  : Parser[String] = """""".r
  def string = "\"" ~> validStringContents <~ "\"" ^^ {case str => new ASTStringLiteral(str)}

  def loadStatement = ((typedRelationIdentifier <~ "<-") <~ "load") ~ ("(" ~> string) ~ (("," ~> format) <~ ")") ^^
    {case id~file~fmt => ASTLoadFileStatement(id, file, fmt)}

  def expression : Parser[ASTExpression] = countExpression | joinAndSelectExpression | identifier | string
  def countExpression : Parser[ASTCount] = "COUNT" ~> "(" ~> (expression <~ ")") ^^ {case e => ASTCount(e)}
  def joinAndSelectExpression = joinExpression ^^ {case e => ASTJoinAndSelect(e._1, e._2)}
  def joinExpression : Parser[(List[ASTRelation],List[ASTCriterion])] = notLastJoinExpression | lastJoinExpression
  def notLastJoinExpression = relationIdentifier ~ ("," ~> (joinExpression|selectExpression)) ^^ {case id~rest => (id::rest._1, rest._2)}
  def selectExpression : Parser[(List[ASTRelation],List[ASTCriterion])] = notLastSelectExpression | lastSelectExpression
  def notLastSelectExpression = select  ~ ("," ~> selectExpression) ^^ {case criterion~rest => (rest._1, criterion::rest._2)}
  def lastJoinExpression = relationIdentifier ^^ {case id => (List[ASTRelation](id), List[ASTCriterion]())}
  def lastSelectExpression = select ^^ {case criterion => (List[ASTRelation](), List[ASTCriterion](criterion))}

  def select = eqSelect | leqSelect | geqSelect | lSelect | gSelect | neqSelect
  def eqSelect = expression ~ ("=" ~> expression) ^^ {case e1~e2 => ASTEq(e1, e2)}
  def leqSelect = expression ~ ("<=" ~> expression) ^^ {case e1~e2 => ASTLeq(e1, e2)}
  def geqSelect = expression ~ (">=" ~> expression) ^^ {case e1~e2 => ASTGeq(e1, e2)}
  def lSelect = expression ~ ("<" ~> expression) ^^ {case e1~e2 => ASTLess(e1, e2)}
  def gSelect = expression ~ (">" ~> expression) ^^ {case e1~e2 => ASTGreater(e1, e2)}
  def neqSelect = expression ~ ("!=" ~> expression) ^^ {case e1~e2 => ASTNeq(e1, e2)}

  def assignStatement = identifier ~ ("<-" ~> expression) ^^ { case id~e => ASTAssignStatement(id, e) }
  def printStatement = "print" ~> expression ^^ {case e => ASTPrintStatement(e)}
  def statement = loadStatement | assignStatement | printStatement
  def statements = rep(statement) ^^ {case list => new ASTStatements(list)}
}
