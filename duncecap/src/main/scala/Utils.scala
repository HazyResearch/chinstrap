package DunceCap

import scala.collection.mutable
import sys.process._
import scala.io._
import java.nio.file.{Paths, Files}
import java.io.{FileWriter, File, BufferedWriter}
import argonaut.Argonaut._
import argonaut.Json

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
  def loadEnvironmentFromJSON(config:Map[String,Any], create_db:Boolean, db_folder:String):Unit = {
    //Pull top level vars from JSON file
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
      val source = if(create_db) r("source").asInstanceOf[String] else ""
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

      val attributePositions = (0 until attributes.size).toList
      val orderings = try { 
        r("orderings").asInstanceOf[List[List[Double]]].map(e => e.map(_.toInt))
      } catch{
        case _:Throwable => attributePositions.permutations.toList
      }

      orderings.foreach(o_attrs => {    
        val o_aname = o_attrs.mkString("_")
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

  def writeEnvironmentToJSON():Unit = {
    val file = new File(Environment.dbPath+"/config.json")
    val bw = new BufferedWriter(new FileWriter(file))
    
    //PREPARE THE RELATIONS TO SPILL TO JSON
    val rJson = Environment.relations.map(tup =>{
      val (name,r) = tup
      val orderings = r.map( tup2 => {
        val (oname,or) = tup2
        jArray(oname.split("_").tail.map(c => {
          jNumber(c.toInt)
        }).toList)
      }).toList
      Json("name" -> jString(name),
          "orderings" -> jArray(orderings),
          "attributes" -> jArray(
            (0 until r.head._2.types.length).map(i => {
              Json("type" -> jString(r.head._2.types(i)),
                  "encoding" -> jString(r.head._2.encodings(i))
              )
            }).toList
          )
      )
    }).toList

    //top level json struct
    val json = 
        Json("numThreads" -> jNumber(Environment.numThreads),
          "numNUMA" -> jNumber(Environment.numNUMA),
          "algorithm" -> jString(Environment.algorithm),
          "layout" -> jString(Environment.layout),
          "relations" ->  jArray(rJson)
        )

    bw.append(json.spaces2)
    bw.close()
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

    if(Environment.quiet) print("Compiling C++ code....")
    val silent = if(Environment.quiet) "--silent" else "SILENT=false"
    sys.process.Process(Seq("rm", "-rf",s"""bin/${filename}"""), new File("emptyheaded")).!
    val result = sys.process.Process(Seq("make",silent,s"""NUM_THREADS=${Environment.numThreads}""", s"""bin/${filename}"""), new File("emptyheaded")).!

    if (result != 0) {
      println("FAILURE: Compilation errors.")
      System.exit(1)
    }
    if(Environment.quiet) println("FINISHED. Running C++ code....")

    s"""emptyheaded/bin/${filename}""" !
  }
}