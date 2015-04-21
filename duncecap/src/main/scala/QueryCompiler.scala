package DunceCap

import java.io.{FileWriter, File, BufferedWriter}
import sys.process._
import java.nio.file.{Paths, Files}

/**
 * You can compile the output of this class by doing
 *
 * clang++ runnable.cpp -o runnable.exe -std=c++11 -march=native -mtune=native -Isrc
 *
 * from the emptyheaded directory
 */
object QueryCompiler extends App {
  val usage = "Usage: ./QueryCompiler query.datalog outputFilename"
  if (args.length != 2) {
    println(usage)
  } else {
    val source = scala.io.Source.fromFile(args.head)
    val outputFilename = args.tail.head
    val lines = try source.mkString finally source.close()
    DCParser.parseAll(DCParser.statements, lines) match {
      case DCParser.Success(ast, _) => {
        // TODO (sctu):  proper error handling in case you can't open the file, etc.
        val codeStringBuilder = new CodeStringBuilder
        CodeGen.emitHeaderAndCodeForAST(codeStringBuilder, ast)
        CodeGen.emitMainMethod(codeStringBuilder)

        val cppDir = "../emptyheaded/generated"
        val cppFilepath = s"""${cppDir}/${outputFilename}.cpp"""
        val binaryFilepath = s"""../emptyheaded/bin/${outputFilename}.exe"""
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

        val result = s"""clang++ ${cppFilepath} -o ${binaryFilepath} -std=c++11 -march=native -mtune=native -I../emptyheaded/src""" !

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
