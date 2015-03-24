package DunceCap

//import DunceCap.Interpreter.eval
//import DunceCap.Parser.parse

object repl extends App {

  def commandLoop(): Unit = {
    try {
      print("> ")
      val line = readLine()
      println(line)
      commandLoop()
      //(commandLoop _).tupled(eval(env, parse(line).head))
    } catch {
      case e: Exception => commandLoop()
    }
  }

  val greet = "(display \"DunceCap v0.1 \")"
  //val (env, res) = eval(globalEnv, parse(greet).head)
  commandLoop()
}