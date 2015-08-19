package DunceCap

import scala.collection.mutable
import sys.process._
import scala.io._
import java.nio.file.{Paths, Files}
import java.io.{FileWriter, File, BufferedWriter}

class CodeStringBuilder {
  val buffer = new StringBuilder
  def println(str : String): Unit = {
    buffer.append(str + "\n")
  }
  def print(str : String): Unit = {
    buffer.append(str)
  }

  override def toString = {
    buffer.toString
  }
}

object Utils{
  def loadEnvironmentFromJSON(config:Map[String,Any], create_db:Boolean):Unit = {
    //Pull top level vars from JSON file
    val db_folder = config("database").asInstanceOf[String]
    Environment.layout = config("layout").asInstanceOf[String]
    Environment.algorithm = config("algorithm").asInstanceOf[String]
    Environment.numNUMA = config("numNUMA").asInstanceOf[Double].toInt
    Environment.numThreads = config("numThreads").asInstanceOf[Double].toInt
    Environment.dbPath = db_folder

    if(create_db){
      //setup folders for the database on disk
      s"""mkdir -p ${db_folder} ${db_folder}/relations ${db_folder}/encodings""" !
    }
    
    val relations = config("relations").asInstanceOf[List[Map[String,Any]]]
    relations.foreach(r => {
      val name = r("name").asInstanceOf[String]
      val source = r("source").asInstanceOf[String]
      val attributes = r("attributes").asInstanceOf[List[Map[String,String]]]

      attributes.foreach(a => { 
        Environment.addEncoding(new Encoding(a("encoding"),a("type")))
        if(create_db)
          s"""mkdir -p ${db_folder}/encodings/${a("encoding")} """ !
      })
      val attribute_types = attributes.map(a => a("type"))
      val attribute_encodings = attributes.map(a => a("encoding"))

      if(create_db){
        val master_relation = new Relation(name,attribute_types,attribute_encodings)
        Environment.addASTNode(ASTLoadRelation(source,master_relation))
      }

      val ordering = r("ordering").asInstanceOf[List[String]]
      val attributePositions = (0 until attributes.size).toList
      val orderings = if(ordering.length == 1 && ordering(0) == "all") attributePositions.permutations.toList else List(ordering)
      orderings.foreach(o_attrs => {    
        val o_aname = o_attrs.mkString("")
        val o_name = s"""${name}_${o_aname}""" 
        val o_types = o_attrs.map(a => attribute_types(attributePositions.indexOf(a)) )
        val o_encod = o_attrs.map(a => attribute_encodings(attributePositions.indexOf(a)) )
        val relationIn = new Relation(o_name,o_types,o_encod)
        Environment.addRelation(name,relationIn)
        if(create_db){
          Environment.addASTNode(ASTBuildTrie(source,relationIn,name))
          s"""mkdir ${db_folder}/relations/${o_name}""" !
        }
      })
    })
  }

  def compileAndRun(codeStringBuilder:CodeStringBuilder,filename:String):Unit = {
    val cppDir = "emptyheaded/generated"
    val cppFilepath = s"""${cppDir}/${filename}.cpp"""
    if (!Files.exists(Paths.get(cppDir))) {
      println(s"""Making directory ${cppDir}""")
      s"""mkdir ${cppDir}""" !
    }

    val file = new File(cppFilepath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(codeStringBuilder.toString)
    bw.close()
    s"""clang-format -style=llvm -i ${file}""" !

    sys.process.Process(Seq("rm", "-rf",s"""bin/${filename}"""), new File("emptyheaded")).!
    val result = sys.process.Process(Seq("make",s"""NUM_THREADS=${Environment.numThreads}""", s"""bin/${filename}"""), new File("emptyheaded")).!

    if (result != 0) {
      println("FAILURE: Compilation errors.")
      System.exit(1)
    }

    s"""emptyheaded/bin/${filename}""" !
  }
}