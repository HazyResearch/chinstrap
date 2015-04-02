#ifndef _TRIE_H_
#define _TRIE_H_

#include "Level.hpp"
#include <unordered_map>

template <class T>
class Trie{
public:
  std::vector<std::vector<T>*> *attributes;
  
  std::unordered_map<T,uint32_t> *key_to_value;
  std::vector<uint32_t> value_to_key;

  Trie(std::vector<std::vector<T>*> *attr_in){
    attributes = attr_in;
  }

  static Trie<T> build(std::vector<std::vector<T>*> *attr_in);

  //for debug purposes
  void print();
};

#endif