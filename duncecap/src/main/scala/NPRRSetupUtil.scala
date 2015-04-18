package DunceCap

import scala.collection.mutable

object NPRRSetupUtil {

  type Relation = (String, List[String])
  type Relations = List[Relation]
  type RWRelation = (String, List[Int]) // rewritten relation, i.e., attr names have been replaced by 0,1,2, etc.
  type RWRelations = List[RWRelation]

  type RName = String
  type RIndex = Int
  type Column = (RName, RIndex)
  type EquivalenceClass = List[(RName, RIndex)]

  private def DFS(visited : mutable.Set[Column], start : Column, relations : Relations) : List[Column] = {
    assert(!visited.contains(start))
    visited.add(start)

    val letters = relations.filter((rel : Relation) => rel._1 == start._1).map((rel : Relation) => {
      rel._2(start._2)
    }).toSet

    relations.flatMap((rel : Relation) => {
      rel._2.zipWithIndex.foldLeft(List[Column](start))((accum: List[Column],  attrAndIndex : (String, RIndex)) =>{
        if (letters.contains(attrAndIndex._1) && !visited.contains((rel._1, attrAndIndex._2))) {
          accum ++ DFS(visited, (rel._1, attrAndIndex._2), relations)
        } else {
          accum
        }
      })
    }).distinct // TODO make less gross
  }

  def buildEncodingEquivalenceClasses(relations : Relations): List[List[(RName, RIndex)]] = {
    val visited = mutable.Set[(RName, RIndex)]()
    relations.flatMap((rel : Relation) => {
      (0 until rel._2.size).toList.flatMap((index : RIndex) => {
        if (!visited.contains((rel._1, index))) {
          Some(DFS(visited, (rel._1, index), relations))
        } else {
          None
        }
      })
    })
  }

  /**
   * @return Columns relevant to the encoding of any columns named attr
   */
  def getEncodingsRelevantToAttr(targetAttr : String, relations : Relations, encodings : List[EquivalenceClass]) : Set[Column]= {
    relations.flatMap((rel : Relation) => {
      rel._2.zipWithIndex.flatMap((attrAndIndex : (String, Int)) => {
        val (attr, index) = attrAndIndex
        if (attr == targetAttr) {
          Some(encodings.flatMap((klass : EquivalenceClass) => {
            if (klass.contains((rel._1, index))) {
              Some((klass.head._1, klass.head._2))
            } else {
              None
            }
          }).distinct)
        } else {
          None
        }
      }).flatten
    }).toSet
  }


  def mkEncodingName(col : Column) = {
    col._1 + "_" + col._2
  }
  def mkEncodingName(klass : EquivalenceClass) = {
    klass.head._1 + "_" + klass.head._2
  }

  def getDistinctRelations(relations : Relations): RWRelations = {
    val relationsGroupedByName = relations.groupBy((relation : Relation) => relation._1).toList
    return relationsGroupedByName
      .map((rels : (String, List[Relation])) => {rels._2.head})
      .map((rel : Relation) => (rel._1, (0 until rel._2.size).toList))
  }
}
