package DunceCap

import java.io.{FileWriter, File, BufferedWriter}

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
        val file = new File(s"""../emptyheaded/${outputFilename}""")
        val bw = new BufferedWriter(new FileWriter(file))
        bw.write(codeStringBuilder.toString)
        bw.close()
      }
      case x => println(x)
    }
  }
}
