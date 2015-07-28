#ifndef _TRIEBLOCK_H_
#define _TRIEBLOCK_H_

#include "set/ops.hpp"

template<class T, class F>
struct TrieBlock{
  Set<T> set;
  TrieBlock<T,F>** next_level;
  F* values;
  bool is_sparse;

  TrieBlock(TrieBlock *init){
    set = init->set;
    next_level = init->next_level;
    is_sparse = init->is_sparse;
    values = init->values;
  }

  TrieBlock(){
    values = NULL;
  }
  TrieBlock(bool sparse){
    is_sparse = sparse;
  }

  void init_pointers(size_t tid, allocator::memory<uint8_t> *allocator_in, const size_t cardinality, const size_t range, const bool is_sparse_in){
    is_sparse = is_sparse_in;
    if(!is_sparse){
      next_level = (TrieBlock<T,F>**)allocator_in->get_next(tid, sizeof(TrieBlock<T,F>*)*(range+1) );
    } else{
      next_level = (TrieBlock<T,F>**)allocator_in->get_next(tid, sizeof(TrieBlock<T,F>*)*cardinality);
    }
  }

  void alloc_data(size_t tid, allocator::memory<uint8_t> *allocator_in, const size_t cardinality, const size_t range){
    if(!is_sparse){
      values = (F*)allocator_in->get_next(tid, sizeof(F)*(range+1));
    } else{
      values = (F*)allocator_in->get_next(tid, sizeof(F)*cardinality);
    }
  }

  void init_data(const size_t tid, allocator::memory<uint8_t> *allocator_in, const size_t cardinality, const size_t range, const F value){
    if(!is_sparse){
      values = (F*)allocator_in->get_next(tid, sizeof(F)*(range+1));
      std::fill(values,values+range+1,value);
    } else{
      values = (F*)allocator_in->get_next(tid, sizeof(F)*(cardinality));
      std::fill(values,values+cardinality,value);
    }
  }

  void set_data(uint32_t index, uint32_t data, F value){
    if(!is_sparse){
      (void) index;
      values[data] = value;
    } else{
      (void) data;
      values[index] = value;
    }
  }

  F get_data(uint32_t data){
    if(!is_sparse){
      return values[data];
    } else{
      //something like get the index from the set then move forward.
      const long index = set.find(data);
      if(index != -1)
        return values[data];
    }
    //FIXME
    return (F)0;
  }

  F get_data(uint32_t index, uint32_t data){
    if(!is_sparse){
      (void) index;
      return values[data];
    } else{
      (void) data;
      return values[index];
    }
  }

  void set_block(uint32_t index, uint32_t data, TrieBlock<T,F> *block){
    if(!is_sparse){
      (void) index;
      next_level[data] = block;
    } else{
      (void) data;
      next_level[index] = block;
    }
  }

  inline std::tuple<size_t,TrieBlock<T,F>*> get_block_forward(size_t index, uint32_t data) const{
    if(!is_sparse){
      return std::make_tuple(index,next_level[data]);
    } else{
      //something like get the index from the set then move forward.
      auto tup = set.find(index,data);
      size_t find_index = std::get<0>(tup);
      if(std::get<1>(tup))
        return std::make_tuple(find_index,next_level[find_index]);
      else
        return std::make_tuple(find_index,(TrieBlock<T,F>*)NULL);
    }
  }
  inline TrieBlock<T,F>* get_block(uint32_t data) const{
    TrieBlock<T,F>* result = NULL;
    if(!is_sparse){
      result = next_level[data];
    } else{
      //something like get the index from the set then move forward.
      const long index = set.find(data);
      if(index != -1)
        result = next_level[index];
    }
    return result;
  }
  inline TrieBlock<T,F>* get_block(uint32_t index, uint32_t data) const{
    if(!is_sparse){
      return next_level[data];
    } else{
      return next_level[index];
    }
    return NULL;
  }

};

template<class T,class F>
struct TrieBlockIterator{
  size_t pointer_index;
  TrieBlock<T,F>* trie_block;

  TrieBlockIterator(TrieBlock<T,F> *init){
    pointer_index = 0;
    trie_block = init;
  }

  TrieBlockIterator<T,F> get_block(uint32_t data) {
    auto tup = trie_block->get_block_forward(0,data);
    pointer_index = std::get<0>(tup);
    return TrieBlockIterator(std::get<1>(tup));
  }
};

#endif
