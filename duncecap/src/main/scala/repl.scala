package DunceCap

object repl extends App {
  def commandLoop(): Unit = {
    try {
      print("> ")
      val line = readLine()
      println(line)
      c.send()
      commandLoop()
      //(commandLoop _).tupled(eval(env, parse(line).head))
    } catch {
      case e: Exception => commandLoop()
    }
  }

  val c = new Client()
  c.init()
  commandLoop()
}