package DunceCap

import scala.util.parsing.combinator.RegexParsers

class DCParser extends RegexParsers {
  def identifier = """[_\p{L}][_\p{L}\p{Nd}]*""".r
  def typename = """long|string|float""".r // TODO: should I wrap this in another type or just let it be a string?
  def format = """csv|tsv""".r

  def identifierWithType = identifier ~ (("(" ~> attrDefList) <~ ")") ^^ {case id~attrs => ASTRelation(attrs)}
  def attrDefList : Parser[Map[String, String]] = notLastAttr | lastAttr
  def notLastAttr = identifier ~ (":" ~> typename) ~ ("," ~> attrDefList) ^^ {case a~t~rest => rest + (a -> t)}
  def lastAttr = identifier ~ (":" ~> typename) ^^ {case a~t => Map(a -> t)}

  def notQuote : Parser[String] = """[^\"]""".r
  def escapedQuote : Parser[String] = """\\\"""".r
  def validStringContents : Parser[String] = notEndOfString | endOfString
  def notEndOfString  : Parser[String] = ((escapedQuote|notQuote) ~ validStringContents) ^^ {case c~rest => c+rest}
  def endOfString  : Parser[String] = """""".r
  def string = "\"" ~> validStringContents <~ "\""

  def loadExpr = ((identifierWithType <~ "<-") <~ "load") ~ ("(" ~> string) ~ (("," ~> format) <~ ")") ^^
    {case id~file~fmt => ASTLoadExpression(id, file, fmt)}
}

abstract trait ASTExpression
case class ASTRelation(attrs : Map[String, String]) extends ASTExpression // attribute name to type
case class ASTLoadExpression(rel : ASTRelation, filename : String, format : String) extends ASTExpression