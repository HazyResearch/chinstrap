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

  TrieBlock(){}
  TrieBlock(bool sparse){
    is_sparse = sparse;
  }

  void init_pointers(size_t tid, allocator::memory<uint8_t> *allocator_in, const size_t cardinality, const size_t range, const bool is_sparse_in){
    is_sparse = is_sparse_in;
    if(!is_sparse){
      next_level = (TrieBlock<T>**)allocator_in->get_next(tid, sizeof(TrieBlock<T>*)*range);
    } else{
      next_level = (TrieBlock<T>**)allocator_in->get_next(tid, sizeof(TrieBlock<T>*)*cardinality);
    }
  }

  void set_block(uint32_t index, uint32_t data, TrieBlock<T> *block){
    if(!is_sparse){
      (void) index;
      next_level[data] = block;
    } else{
      (void) data;
      next_level[index] = block;
    }
  }

  inline std::tuple<size_t,TrieBlock<T>*> get_block_forward(size_t index, uint32_t data) const{
    if(!is_sparse){
      return std::make_tuple(index,next_level[data]);
    } else{
      //something like get the index from the set then move forward.
      auto tup = set.find(index,data);
      size_t find_index = std::get<0>(tup);
      if(std::get<1>(tup))
        return std::make_tuple(find_index,next_level[find_index]);
      else
        return std::make_tuple(find_index,(TrieBlock<T>*)NULL);
    }
  }
  TrieBlock<T>* get_block(uint32_t data) const{
    if(!is_sparse){
      return next_level[data];
    } else{
      //something like get the index from the set then move forward.
      const long index = set.find(data);
      if(index != -1)
        return next_level[index];
    }
    return NULL;
  }
  TrieBlock<T>* get_block(uint32_t index, uint32_t data) const{
    if(!is_sparse){
      return next_level[data];
    } else{
      return next_level[index];
    }
    return NULL;
  }

};

template<class T>
struct TrieBlockIterator{
  size_t pointer_index;
  TrieBlock<T>* trie_block;

  TrieBlockIterator(TrieBlock<T> *init){
    pointer_index = 0;
    trie_block = init;
  }

  TrieBlockIterator<T> get_block(uint32_t data) {
    auto tup = trie_block->get_block_forward(0,data);
    pointer_index = std::get<0>(tup);
    return TrieBlockIterator(std::get<1>(tup));
  }
};

#endif
