#ifndef _TRIE_H_
#define _TRIE_H_

#include "Block.hpp"
#include "Encoding.hpp"
#include <unordered_map>
#include <map>

class Trie{
public:
  std::vector<std::vector<Block*>*> *levels;
    
  template<typename F>
  static Trie* build(std::vector<Column<uint32_t>*> *attr_in, F f);

  Trie(std::vector<std::vector<Block*>*> *levels_in){
    levels = levels_in;
  }
};

std::pair<std::vector<std::multimap<uint32_t,uint32_t>*>*,std::vector<Block*>*> encode(
  const size_t max_size,
  Column<uint32_t> *cur_column,
  std::vector<std::multimap<uint32_t,uint32_t>*> *mymultimaps, 
  std::vector<Block*> *blocks){
  //encode sets out of multimaps
  //build hash maps

  std::vector<std::multimap<uint32_t,uint32_t>*> *outputmultimaps = new std::vector<std::multimap<uint32_t,uint32_t>*>();
  std::vector<Block*> *outblocks = new std::vector<Block*>();
  //maybe use a vector of blocks instead of a vector of block pointers
  uint32_t *new_set = new uint32_t[max_size];
  for(size_t i = 0; i < mymultimaps->size(); i++){
    int prev = -1;
    std::multimap<uint32_t,uint32_t> *mymultimap = mymultimaps->at(i);

    std::multimap<uint32_t,uint32_t>*outmap = NULL;
    Block *outblock = NULL;
    size_t new_set_size = 0;

    for (auto it=mymultimap->begin(); it!=mymultimap->end(); ++it){
      if((*it).first != prev){
        if(outblock != NULL && outmap != NULL){
          outputmultimaps->push_back(outmap);
          outblocks->push_back(outblock);
        }
        outblock = new Block();
        outmap = new std::multimap<uint32_t,uint32_t>();
        prev = (*it).first;
        new_set[new_set_size++] = (*it).first;
        blocks->at(i)->map->insert(std::pair<uint32_t,Block*>((*it).first,outblock));
      }
      if(cur_column != NULL){
        outmap->insert(std::pair<uint32_t,uint32_t>(cur_column->at((*it).second),(*it).second));
      }
    }

    outputmultimaps->push_back(outmap);
    outblocks->push_back(outblock);

    //hack
    uint8_t *set_data_in = new uint8_t[new_set_size*sizeof(uint64_t)];
    blocks->at(i)->data = Set<uinteger>::from_array(set_data_in,new_set,new_set_size);
  }

  return make_pair(outputmultimaps,outblocks);
}


template<typename F>
inline Trie* Trie::build(std::vector<Column<uint32_t>*> *attr_in,
  F f){

  const size_t num_levels = attr_in->size();

  //special code to encode the first level
  std::vector<std::multimap<uint32_t,uint32_t>*> *mymultimaps = new std::vector<std::multimap<uint32_t,uint32_t>*>(); 
  size_t cur_level = 0;
  const Column<uint32_t> * const cur_attributes = attr_in->at(cur_level);
  const size_t num_attributes = cur_attributes->size();
  //parallel for
  std::multimap<uint32_t,uint32_t>* mymultimap = new std::multimap<uint32_t,uint32_t>(); 
  for(size_t i = 0; i < num_attributes; i++){
    if(f(i))
      mymultimap->insert(std::pair<uint32_t,uint32_t>(cur_attributes->at(i),i));
  }
  cur_level++;
  mymultimaps->push_back(mymultimap);

  std::vector<Block*> *blocks = new std::vector<Block*>();
  blocks->push_back(new Block());

  //////////////////////////////////////////////////////////////////////////////////////////////////////
  //Encode the remaining levels
  typedef std::pair<std::vector<std::multimap<uint32_t,uint32_t>*>*,std::vector<Block*>*> level_pair;
  std::vector<std::vector<Block*>*> *levels_in = new std::vector<std::vector<Block*>*>();
  levels_in->push_back(blocks);
  for(; cur_level < num_levels; cur_level++){
    level_pair out_pair = encode(num_attributes,attr_in->at(cur_level),mymultimaps,blocks);
    mymultimaps = out_pair.first;
    blocks = out_pair.second;
    levels_in->push_back(blocks);
  }
  level_pair out_pair = encode(num_attributes,NULL,mymultimaps,blocks);
  mymultimaps = out_pair.first;
  blocks = out_pair.second;
  levels_in->push_back(blocks);

  return new Trie(levels_in);
}
#endif