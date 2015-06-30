package DunceCap

import java.io.{FileWriter, File, BufferedWriter}
import sys.process._
import java.nio.file.{Paths, Files}
import scala.util.matching.Regex

/**
 * You can run the output of this class by doing
 *
 * ./bin/outputFilename
 *
 * from the emptyheaded/bin directory
 */
object QueryCompiler extends App {
  val usage = "Usage: ./QueryCompiler QQuery.datalog"
  if (args.length != 1) {
    println(usage)
  } else {
    val input_file = if(args.head.head.equals("/")) args.head else ("../" + args.head)
    val source = scala.io.Source.fromFile(input_file)
    val ParseFile = """(.*)/(.*).datalog""".r
    val ParseFile(path,fname) = input_file
    println("HERE: " + path + " " + fname)
    val outputFilename = fname
    val lines = try source.mkString finally source.close()
    DCParser.parseAll(DCParser.statements, lines) match {
      case DCParser.Success(ast, _) => {
        // TODO (sctu):  proper error handling in case you can't open the file, etc.
        val codeStringBuilder = new CodeStringBuilder
        CodeGen.emitHeaderAndCodeForAST(codeStringBuilder, ast)
        CodeGen.emitMainMethod(codeStringBuilder)

        val cppDir = "../emptyheaded/generated"
        val cppFilepath = s"""${cppDir}/${outputFilename}.cpp"""
        if (!Files.exists(Paths.get(cppDir))) {
          println(s"""Making directory ${cppDir}""")
          s"""mkdir ${cppDir}""" !
        } else {
          println(s"""Found directory ${cppDir}, continuing...""")
        }

        val file = new File(cppFilepath)
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(codeStringBuilder.toString)
        bw.close()
        s"""clang-format -i ${file}""" !

        sys.process.Process(Seq("make", "clean"), new File("../emptyheaded")).!
        val result = sys.process.Process(Seq("make", s"""bin/${outputFilename}"""), new File("../emptyheaded")).!

        if (result != 0) {
          println("FAILURE: Compilation errors.")
        } else {
          println("SUCCESS.")
        }
      }
      case x => println(x)
    }
  }
}
