package DunceCap

import scala.collection.mutable

object NPRRSetupUtil {
  type Relations = List[Relation]

  // rewritten relation, i.e., attr names have been replaced by 0,1,2, etc.
  class RWRelation(val attrs: List[Int], val name: String) {
    override def equals(that: Any): Boolean =
      that match {
        case that: RWRelation => that.attrs.equals(attrs) && that.name.equals(name)
        case _ => false
      }
  }

  type RWRelations = List[RWRelation]

  type RName = String
  type RIndex = Int
  type Column = (RName, RIndex)
  type EquivalenceClass = List[(RName, RIndex)]

  private def DFS(visited : mutable.Set[Column], start : Column, relations : Relations) : List[Column] = {
    assert(!visited.contains(start))
    visited.add(start)

    val letters = relations.filter((rel : Relation) => rel.name == start._1).map((rel : Relation) => {
      rel.attrs(start._2)
    }).toSet

    relations.foldLeft(List[Column](start))((accum: List[Column], rel : Relation) => {
      accum ++ rel.attrs.zipWithIndex.flatMap((attrAndIndex : (String, RIndex)) =>{
        if (letters.contains(attrAndIndex._1) && !visited.contains((rel.name, attrAndIndex._2))) {
          Some(DFS(visited, (rel.name, attrAndIndex._2), relations))
        } else {
          None
        }
      }).flatten
    })
  }

  def buildEncodingEquivalenceClasses(relations : Relations): List[List[(RName, RIndex)]] = {
    val visited = mutable.Set[(RName, RIndex)]()
    relations.flatMap((rel : Relation) => {
      (0 until rel.attrs.size).toList.flatMap((index : RIndex) => {
        if (!visited.contains((rel.name, index))) {
          Some(DFS(visited, (rel.name, index), relations))
        } else {
          None
        }
      })
    })
  }

  /**
   * @return Columns relevant to the encoding of any columns named attr
   */
  def getEncodingRelevantToAttr(targetAttr : String, relations : Relations, encodings : List[EquivalenceClass]) : Column = {
    val cols = relations.flatMap((rel : Relation) => {
      rel.attrs.zipWithIndex.flatMap((attrAndIndex : (String, Int)) => {
        val (attr, index) = attrAndIndex
        if (attr == targetAttr) {
          Some(encodings.flatMap((klass : EquivalenceClass) => {
            if (klass.contains((rel.name, index))) {
              Some((klass.head._1, klass.head._2))
            } else {
              None
            }
          }).distinct)
        } else {
          None
        }
      }).flatten
    }).distinct

    assert(cols.size == 1) // if not, true, equivalence classes were not properly constructed
    return cols.head
  }


  def mkEncodingName(col : Column) = {
    col._1 + "_" + col._2
  }
  def mkEncodingName(klass : EquivalenceClass) = {
    klass.head._1 + "_" + klass.head._2
  }

  def getDistinctRelations(relations : Relations): RWRelations = {
    val relationsGroupedByName = relations.groupBy((relation : Relation) => relation.name).toList
    return relationsGroupedByName
      .map((rels : (String, Relations)) => {rels._2.head})
      .map((rel : Relation) => new RWRelation((0 until rel.attrs.size).toList, rel.name))
  }
}
