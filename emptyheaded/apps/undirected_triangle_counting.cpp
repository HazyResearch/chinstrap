#include "main.hpp"
//#include <tbb/tbb.h>

template<class T, class R>
class undirected_triangle_counting: public application<T,R> {
  void run(){
    //create the relation (currently a column wise table)
    Relation<uint64_t,uint64_t> R_ab;

//////////////////////////////////////////////////////////////////////
    //File IO (for a tsv, csv should be roughly the same)
    tsv_reader f_reader("simple.txt");
    char *next = f_reader.tsv_get_first();
    R_ab.num_columns = 0;
    while(next != NULL){
      //have to code generate number of attributes here
      //maybe can accomplish with variadic templates? Might be hard.
      R_ab.get<0>()->append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab.get<1>()->append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab.num_columns++;
    }

//////////////////////////////////////////////////////////////////////
    //Encoding

    //encode A
    std::vector<Column<uint64_t>*> *a_attributes = new std::vector<Column<uint64_t>*>();
    a_attributes->push_back(R_ab.get<0>());
    a_attributes->push_back(R_ab.get<1>());
    Encoding<uint64_t> a_encoding(a_attributes);

    //encode B
    /*
    std::vector<Column<uint64_t>*> *b_attributes = new std::vector<Column<uint64_t>*>();
    b_attributes->push_back(R_ab.get<1>());
    Encoding<uint64_t> b_encoding(b_attributes);
    */
//////////////////////////////////////////////////////////////////////
    //Trie building

    //after all encodings are done, set up encoded relation (what is passed into the Trie)
    //You can switch the ordering here to be what you want it to be in the Trie.
    //A mapping will need to be kept in the query compiler so it is known what
    //encoding each level in the Trie should map to.
    std::vector<Column<uint32_t>*> *ER_ab = new std::vector<Column<uint32_t>*>();
    ER_ab->push_back(a_encoding.encoded->at(0)); //perform filter, selection
    ER_ab->push_back(a_encoding.encoded->at(1));

    //add some sort of lambda to do selections 
    Trie TR_ab(ER_ab);

//////////////////////////////////////////////////////////////////////
    //Prints the relation
    
    size_t num_triangles = 0;
    Block* head = TR_ab.levels->at(0)->at(0);
    head->data->foreach([&](uint32_t d1){
      Block *l2 = head->map->at(d1);
      Set<uinteger> C(new uint8_t[ER_ab->at(0)->size()]);
      l2->data->foreach([&](uint32_t d2){
        if(head->map->find(d2) != head->map->end())
          num_triangles += ops::set_intersect(&C,l2->data,head->map->at(d2)->data)->cardinality;
      });
    });

    std::cout << num_triangles << std::endl;
  
//////////////////////////////////////////////////////////////////////

  }
};

template<class T, class R>
application<T,R>* init_app(){
  return new undirected_triangle_counting<T,R>(); 
}