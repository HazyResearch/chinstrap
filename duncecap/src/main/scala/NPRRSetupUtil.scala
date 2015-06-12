package DunceCap

import scala.collection.mutable

object NPRRSetupUtil {
  type Relations = List[Relation]
  type RName = String
  type RIndex = Int
  type Column = (RName, RIndex)
  type EquivalenceClass = (String,List[(String, Int)])
  type EquivalenceClasses = Map[String,List[(String, Int)]]


  def buildEncodingEquivalenceClasses(relations : Relations): EquivalenceClasses = {
    val triples = relations.flatMap{(rel: Relation) => 
      (0 until rel.attrs.size).toList.map{ i:Int =>
        (rel.attrs(i),rel.name,i)
      }
    } //pairs will contain (attribute,relation,attribute_index)

    triples.groupBy{ trip =>
      trip._1
    }.map(e => (e._1,e._2.map(a => (a._2,a._3) )))
  }
}
