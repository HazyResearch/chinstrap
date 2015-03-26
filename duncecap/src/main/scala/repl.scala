package DunceCap

import DunceCap.Parser.parseLine

object Repl extends App {
  def commandLoop(): Unit = {
    try {
      print("> ")
      val line = readLine()
      parseLine(line)
      c.send()
      commandLoop()
    } catch {
      case e: Exception => commandLoop()
    }
  }

  val c = new Client()
  c.init()
  commandLoop()
}