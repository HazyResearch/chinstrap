package DunceCap

import java.io.{InputStreamReader, BufferedReader}

import scala.tools.nsc.interpreter.{JPrintWriter, ILoop}
import scala.tools.nsc.Settings

/*class Repl {
  private var inDunceCapMode = true
  private val c = new Client()
  c.init()

}*/

object Repl extends App {
  val repl = new Repl(new BufferedReader(new InputStreamReader(System.in)), new JPrintWriter(System.out, true))
  val settings = new Settings
  settings.usejavacp.value = true
  settings.deprecation.value = true
  repl.repl.process(settings)
}

class Repl(in0: BufferedReader, out: JPrintWriter) {
  /**
   * Sample repl usage as of right now
   * (in :dc mode, prints AST if it parses as something in our query language, in :scala mode, runs scala code).
   *
   * Starting the DunceCap repl...
   *
   * ==> R(a:long,b:string) <- load("file.txt",csv)
   * ASTLoadExpression(ASTRelation(Map(b -> string, a -> long)),file.txt,csv)
   * ==> 2 * 3
   * [1.1] failure: string matching regex `[_\p{L}][_\p{L}\p{Nd}]*' expected but `2' found
   *
   * 2 * 3
   * ^^
   * ==> :scala
   * ==> 2 * 3
   * res0: Int = 6
   * ==> R(a:long,b:string) <- load("file.txt",csv)
   * <console>:1: error: ';' expected but '<-' found.
   *    R(a:long,b:string) <- load("file.txt",csv)
   *                       ^^
   * ==> :dc
   * ==> R(a:long,b:string) <- load("file.txt",csv)
   * ASTLoadExpression(ASTRelation(Map(b -> string, a -> long)),file.txt,csv)
   * ==>
   */

  private var inDunceCapMode = true
  private val c = new Client()
  c.init()

  def repl = new ILoop(in0, out) {
    override def prompt = "==> "

    override def printWelcome() {
      echo("\n" + "Starting the DunceCap repl...\n")
    }

    override def interpretStartingWith(code: String): Option[String] = {
      if (isInDunceCapMode) {
        DCParser.parseAll(DCParser.statement, code) match {
          case DCParser.Success(ast, _) => {
            val codeStringBuilder = new CodeStringBuilder
            CodeGen.emitHeaderAndCodeForAST(codeStringBuilder, ast)
            if (c.send(codeStringBuilder.toString, out)) {
              ast.updateEnvironment
            }
          }
          case x => {
            out.println(x)
          }
        }
        Some(code)
      } else {
        super.interpretStartingWith(code)
      }
    }

    def isInDunceCapMode = inDunceCapMode

    def scalaCmd(): Result = {
      inDunceCapMode = false
      Result(true, None)
    }

    def dunceCapCmd(): Result = {
      inDunceCapMode = true
      Result(true, None)
    }

    import LoopCommand.{ nullary }
    lazy val dunceCapModeCommands = List(
      nullary("scala", "scala mode", scalaCmd),
      nullary("dc", "dunce cap mode", dunceCapCmd))
    override def commands: List[LoopCommand] = dunceCapModeCommands ++ super.commands
  }
}

class CodeStringBuilder {
  val buffer = new StringBuilder
  def println(str : String): Unit = {
    buffer.append(str + "\n")
  }

  override def toString = {
    buffer.toString
  }

}