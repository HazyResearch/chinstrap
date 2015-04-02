#include "main.hpp"

template<class T, class R>
class undirected_triangle_counting: public application<T,R> {
  void run(){
    //create the relation (currently a column wise table)
    Relation<uint64_t,uint64_t> R_ab;

//////////////////////////////////////////////////////////////////////
    //File IO (for a tsv, csv should be roughly the same)
    tsv_reader f_reader("/dfs/scratch0/caberger/datasets/facebook/edgelist/data.txt");
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
    Encoding<uint64_t> a_encoding(a_attributes);

    //encode B
    std::vector<Column<uint64_t>*> *b_attributes = new std::vector<Column<uint64_t>*>();
    b_attributes->push_back(R_ab.get<1>());
    Encoding<uint64_t> b_encoding(b_attributes);

//////////////////////////////////////////////////////////////////////
    //Trie building

    //after all encodings are done, set up encoded relation (what is passed into the Trie)
    //You can switch the ordering here to be what you want it to be in the Trie.
    //A mapping will need to be kept in the query compiler so it is known what
    //encoding each level in the Trie should map to.
    std::vector<Column<uint32_t>*> *ER_ab = new std::vector<Column<uint32_t>*>();
    ER_ab->push_back(a_encoding.encoded->at(0));
    ER_ab->push_back(b_encoding.encoded->at(0));

    //EncodedRelation ER_ab(a_encoding.columns->at(0),b_encoding.columns->at(0));
    //Trie TR_ab(ER_ab);

//////////////////////////////////////////////////////////////////////
    //Prints the relation
    for(size_t i = 0; i < R_ab.num_columns; i++){
      std::cout << a_encoding.value_to_key->at(ER_ab->at(0)->at(i)) << "\t"
      << b_encoding.value_to_key->at(ER_ab->at(1)->at(i)) << "\t"
      << std::endl;
    }
//////////////////////////////////////////////////////////////////////

  }
};

template<class T, class R>
application<T,R>* init_app(){
  return new undirected_triangle_counting<T,R>(); 
}