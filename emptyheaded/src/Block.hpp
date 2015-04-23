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

struct Tail{
  Set<layout> data;
};

struct Block{
  Set<layout> data;
  std::unordered_map<uint32_t,Block*> map;
};

struct Head{
  Set<layout> data;
  Block** map;

  inline Block* get_block(uint32_t index) const {
    return map[index];
  }
  inline void set_block(uint32_t index, Block *b){
    map[index] = b;
  }
};

#endif