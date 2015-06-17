package DunceCap

import scala.collection.mutable

object NPRRSetupUtil {
  type Relations = List[Relation]
  type RName = String
  type RIndex = Int
  type Column = (RName, RIndex)
  type EM = Map[(String,Int),(Int,Int,String)]
  type EquivalenceClasses = (Map[(String,Int),(Int,Int,String)],Map[String,Int],Map[Int,String])

  def buildEncodingEquivalenceClasses(relations : Relations): EquivalenceClasses = {
      /**
     * Figures out aliases amongst relations and attributes, sets up for encoding
     * Code is absolutely horrible
     */
     var equivalentAttributeSets = mutable.Set[mutable.Set[String]]()
     relations.groupBy(r => r.name).foreach{ grp =>
        //stores the index of matching attributes
        var r = mutable.Map[Int,mutable.Set[String]]()
        //stores attributes that share the same names
        var s = mutable.Map[String,mutable.Set[Int]]()

        //build r & s (could probably be written functionally)
        //group by index and group by attribute name
        grp._2.foreach{rel =>
          (0 until rel.attrs.size).toList.map{ i:Int =>
            val a = rel.attrs(i)
            if(!r.contains(i))
              r += (i -> mutable.Set[String]())
            if(!s.contains(a))
              s += (a -> mutable.Set[Int]())
            r(i) += rel.attrs(i)
            s(a) += i
          }
        }

        //Find equivalent attributes
        equivalentAttributeSets ++= s.map{ e =>
          e._2.flatMap{ i => 
            r(i)
          }
        }.toList

      }
      
      println("EQUIV ATTRIBUTES")
      equivalentAttributeSets.foreach{println}

      //Merge all equivalent sets with an equivalent attribute
      //Should just be a list of disjoint sets at the end these are our equivalence classes
      var done = false
      while(!done){
        done = true
        var new_set = mutable.Set[mutable.Set[String]]()
        while(equivalentAttributeSets.size != 0){
          val a1 = equivalentAttributeSets.head
          var a1_set = mutable.Set[String]()
          a1_set ++= a1

          equivalentAttributeSets -= a1
          equivalentAttributeSets.foreach{ a2 =>
            if(a1.intersect(a2).size != 0){
              done = false
              a1_set = a1_set.union(a2)
              equivalentAttributeSets -= a2
            }
          }
          new_set ++= mutable.Set[mutable.Set[String]](a1_set)
        }
        equivalentAttributeSets = new_set
      }

      println("EQUIV ATTRIBUTES")
      equivalentAttributeSets.foreach{println}

      //maps each attribute to an encoding ID
      var attributeToEncoding = mutable.Map[String,Int]()
      equivalentAttributeSets.foreach{ mys =>
        var index = attributeToEncoding.size
        mys.toList.sorted.foreach{ a =>
          attributeToEncoding += (a -> index)
        }
      }
      //maps each encoding ID to a name
      val encodingIDToName = attributeToEncoding.keys.groupBy(e => attributeToEncoding(e)).map(e => (e._1 -> e._2.toList.sorted.mkString("")))

      var e_to_index = mutable.Map[Int,Int]()
      encodingIDToName.keys.toList.foreach{ i =>
        e_to_index += (i -> 0)
      }
      //build a map between the (relation,attr_index) to (encoding,encoding_index)
      var e_to_index2 = mutable.Map[(String,Int),(Int,Int,String)]()
      relations.foreach{rel =>
        (0 until rel.attrs.size).foreach{i =>
          val ei = e_to_index(attributeToEncoding(rel.attrs(i)))
          val a = (rel.name,i)
          println(rel.attrs(i))
          val b = (attributeToEncoding(rel.attrs(i)),ei,Environment.getTypes(rel.name)(i))
          if(!e_to_index2.contains(a)){
            e_to_index2 += ( a -> b )
            e_to_index(attributeToEncoding(rel.attrs(i))) += 1
          }
        }
      }
      
      println("Attribute to Encoding")
      attributeToEncoding.foreach{println}
      println("Encoding to Name")
      encodingIDToName.foreach{println}
      println("Relations to Index")
      e_to_index2.foreach{println}

      (e_to_index2.toMap,attributeToEncoding.toMap,encodingIDToName.toMap)    
  }
}
