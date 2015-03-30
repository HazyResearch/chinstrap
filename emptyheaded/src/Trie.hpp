#include "Relation.hpp"
#include "Level.hpp"
#include <unordered_map>

/*
template <class T>
class Trie{
public:
  Trie(){
    std::vector<Relation> *t_in;
    std::vector<std::vector<T>*> *attributes;
    std::unordered_map<T,uint32_t> *value_to_key; 
    std::vector<T> *key_to_value;
  }

  //for debug purposes
  void print();

  //read a relation from a file
  static Trie from_relation(Relation t_in);
};

template <class T>
void dictionary_encode(std::vector<Relation> *t_in, 
 std::vector<std::vector<T>*> *attributes,
 std::unordered_map<T,uint32_t> *value_to_key, 
 std::vector<T> *key_to_value){

  for(size_t i = 0; i < attributes->size(); i++){
    for(size_t j = 0; j < attributes->at(i)->size(); j ++){
      //Traverse through relation(i) attribute (attributes->at(i)->at(j))

    }
  }
}

//the trie should be able to be build from multiple relations
//the attributes input specifies which attributes from the relations 
//you want to encode
template <class T>
Trie<T> from_relation(std::vector<Relation> *t_in, 
 std::vector<std::vector<T>*> *attributes){

  std::vector<Level> *levels_in = new std::vector<Level>();
  levels_in->reserve(t_in.num_columns);

  //set up data structures for dictionary encoding
  std::unordered_map<T,uint32_t>* value_to_key = new std::unordered_map<T,uint32_t>();
  value_to_key->reserve(t_in.num_rows);
  std::vector<T> *key_to_value = new std::vector<T>();
  key_to_value->reserve(t_in.num_rows);

  //necessary for building and for arbitrary selections after or during join
  std::vector<size_t> *key_to_table_index = new std::vector<size_t>();
  key_to_value->reserve(t_in.num_rows);


}
*/