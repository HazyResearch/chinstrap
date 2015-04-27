#ifndef _BLOCK_H_
#define _BLOCK_H_

#include "set/ops.hpp"

/*
4 types of blocks should exits (head,dense,sparse,and tail)
1. Head should have just an array of pointers
2. Dense should have a set along with an array of pointers to next level
array of pointers is indexed in by ID
3. Sparse should have a set along with an unordered map (for now)
4. Tail should have just the set data

Data should be laid out like 
----------------------------------
| Set | Set Data | Map | Map Data
----------------------------------
*/

template<class T>
struct Tail{
  Set<T> data;
};

template<class T>
struct Block{
  Set<T> data;
  std::unordered_map<uint32_t,Block<T>*> map;
};

template<class T>
struct Head{
  Set<T> data;
  Block<T>** map;

  inline Block<T>* get_block(uint32_t index) const {
    return map[index];
  }
  inline void set_block(uint32_t index, Block<T> *b){
    map[index] = b;
  }
};

#endif