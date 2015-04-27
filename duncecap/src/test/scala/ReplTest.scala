import java.io._

import DunceCap.Repl
import org.scalatest.FunSuite

import scala.tools.nsc.Settings

class ReplTest extends FunSuite {
  test("Check that repl can compile basic queries by checking responses from server ") {
    val input = ":load scripts/triangleOpt.datalog"
    val is : InputStream = new ByteArrayInputStream(input.getBytes())
    val br = new BufferedReader(new InputStreamReader(is))
    val stringWriter = new StringWriter()
    val writer = new PrintWriter(stringWriter)

    val repl = new Repl(br, writer)
    val settings = new Settings
    settings.usejavacp.value = true
    settings.deprecation.value = true
    repl.repl.process(settings)
    val result = stringWriter.toString
    assert(result.contains("6 rows loaded"))
    assert(result.contains("6\n"))
  }
}
