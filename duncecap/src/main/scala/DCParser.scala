package DunceCap

import scala.util.parsing.combinator.RegexParsers

object DCParser extends RegexParsers {
  def identifier = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  def typename = """long|string|float""".r // TODO: should I wrap this in another type or just let it be a string?
  def format = """csv|tsv""".r

  def identifierWithType = identifier ~ (("(" ~> attrDefList) <~ ")") ^^ {case id~attrs => ASTRelation(attrs)}
  def attrDefList : Parser[Map[String, String]] = notLastAttr | lastAttr
  def notLastAttr = identifier ~ (":" ~> typename) ~ ("," ~> attrDefList) ^^ {case a~t~rest => rest + (a -> t)}
  def lastAttr = identifier ~ (":" ~> typename) ^^ {case a~t => Map(a -> t)}


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
  def string = "\"" ~> validStringContents <~ "\""

  def loadExpr = ((identifierWithType <~ "<-") <~ "load") ~ ("(" ~> string) ~ (("," ~> format) <~ ")") ^^
    {case id~file~fmt => ASTLoadExpression(id, file, fmt)}

  def expr = loadExpr // TODO: other expressions
}

abstract trait ASTExpression
case class ASTRelation(attrs : Map[String, String]) extends ASTExpression // attribute name to type
case class ASTLoadExpression(rel : ASTRelation, filename : String, format : String) extends ASTExpression