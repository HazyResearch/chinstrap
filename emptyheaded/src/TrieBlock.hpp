#ifndef _TRIEBLOCK_H_
#define _TRIEBLOCK_H_

#include "set/ops.hpp"

template<class T>
struct TrieBlock{
  Set<T> set;
  TrieBlock<T>** next_level;
  bool is_sparse;

  void set_block(uint32_t index, uint32_t data, TrieBlock<T> *block){
    if(!is_sparse){
      (void) index;
      next_level[data] = block;
    } else{
      (void) data;
      next_level[index] = block;
    }
  }

  TrieBlock<T>* get_block(uint32_t data) const{
    if(!is_sparse){
      return next_level[data];
    } else{
      //something like get the index from the set then move forward.

    }
    return NULL;
  }

};

#endif