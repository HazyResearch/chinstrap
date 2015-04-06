package DunceCap

import scala.util.parsing.combinator.RegexParsers

object DCParser extends RegexParsers {
  
  def identifierName = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  def typename = """long|string|float""".r // TODO: should I wrap this in another type or just let it be a string?
  def format = """csv|tsv""".r

  def typedRelationIdentifier = identifierName ~ (("(" ~> typedAttrList) <~ ")") ^^ {case id~attrs => ASTRelation(id, attrs)}
  def typedAttrList : Parser[Map[String, Option[String]]] = notLastTypedAttr | lastTypedAttr
  def notLastTypedAttr = identifierName ~ (":" ~> typename) ~ ("," ~> typedAttrList) ^^ {case a~t~rest => rest + (a -> Some(t))}
  def lastTypedAttr = identifierName ~ (":" ~> typename) ^^ {case a~t => Map(a -> Some(t))}

  def identifier = relationIdentifier | scalarIdentifier

  def relationIdentifier = identifierName ~ (("(" ~> attrList) <~ ")") ^^ {case id~attrs => ASTRelation(id, attrs)}
  def attrList : Parser[Map[String, Option[String]]] = notLastAttr | lastAttr
  def notLastAttr = identifierName ~ ("," ~> attrList) ^^ {case a~rest => rest + (a -> None)}
  def lastAttr = identifierName ^^ {case a => Map(a -> None)}

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
    {case id~file~fmt => ASTLoadStatement(id, file, fmt)}

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
}

abstract trait ASTStatement
case class ASTLoadStatement(rel : ASTRelation, filename : ASTStringLiteral, format : String) extends ASTStatement
case class ASTAssignStatement(identifier : ASTIdentifier, expression : ASTExpression) extends ASTStatement
case class ASTPrintStatement(expression : ASTExpression) extends ASTStatement

abstract trait ASTExpression extends ASTStatement
case class ASTCount(expression : ASTExpression) extends ASTExpression
case class ASTJoinAndSelect(rels : List[ASTRelation], selectCriteria : List[ASTCriterion]) extends ASTExpression
case class ASTStringLiteral(str : String) extends ASTExpression

abstract trait ASTCriterion extends ASTExpression
case class ASTEq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion
case class ASTLeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion
case class ASTGeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion
case class ASTLess(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion
case class ASTGreater(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion
case class ASTNeq(attr1 : ASTExpression, attr2 : ASTExpression) extends ASTCriterion

abstract trait ASTIdentifier extends ASTExpression
case class ASTRelation(identifierName : String, attrs : Map[String, Option[String]]) extends ASTIdentifier // attribute name to option with type, or no type if it can be inferred
case class ASTScalar(identifierName : String) extends ASTIdentifier

