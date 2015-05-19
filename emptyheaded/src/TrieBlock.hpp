#ifndef _TRIEBLOCK_H_
#define _TRIEBLOCK_H_

#include "set/ops.hpp"

template<class T>
struct TrieBlock{
  Set<T> set;
  TrieBlock<T>** next_level;
  bool is_sparse;

  TrieBlock(TrieBlock *init){
    set = init->set;
    next_level = init->next_level;
    is_sparse = init->is_sparse;
  }

  TrieBlock(bool sparse){
    is_sparse = sparse;
  }

  void init_pointers(size_t tid, allocator::memory<uint8_t> *allocator_in, const size_t range){
    if(!is_sparse){
      next_level = (TrieBlock<T>**)allocator_in->get_next(tid, sizeof(TrieBlock<T>*)*range);
    } else{
      next_level = (TrieBlock<T>**)allocator_in->get_next(tid, sizeof(TrieBlock<T>*)*set.cardinality);
    }
  }

  void set_block(uint32_t index, uint32_t data, TrieBlock<T> *block){
    if(!is_sparse){
      (void) index;
      std::cout << block->set.cardinality;
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
      long index = set.find(data);
      assert(index != -1);
      return next_level[index];
    }
    return NULL;
  }

};

#endif