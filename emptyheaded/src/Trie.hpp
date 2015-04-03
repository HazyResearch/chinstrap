#ifndef _TRIE_H_
#define _TRIE_H_

#include "Block.hpp"
#include <unordered_map>
#include <map>

static
std::pair<std::vector<std::multimap<uint32_t,uint32_t>*>*,std::vector<Block*>*> encode(
  const size_t max_size,
  Column<uint32_t> *cur_column,
  std::vector<std::multimap<uint32_t,uint32_t>*> *mymultimaps, 
  std::vector<Block*> *blocks){
  //encode sets out of multimaps
  //build hash maps

  std::vector<std::multimap<uint32_t,uint32_t>*> *outputmultimaps = new std::vector<std::multimap<uint32_t,uint32_t>*>();
  std::vector<Block*> *outblocks = new std::vector<Block*>();

  std::cout << "in encoding" << std::endl;
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
      outmap->insert(std::pair<uint32_t,uint32_t>(cur_column->at((*it).second),(*it).second));
      std::cout << (*it).first << " => " << (*it).second << '\n';
    }

    //hack
    uint8_t *set_data_in = new uint8_t[max_size*sizeof(uint32_t)];
    blocks->at(i)->data = Set<uinteger>::from_array(set_data_in,new_set,new_set_size);
  }

  return make_pair(outputmultimaps,outblocks);
  //take block from previous level
    //encode it 

  //should return a pair that is the same as the input(next level)
}

class Trie{
public:
  Trie(std::vector<Column<uint32_t>*> *attr_in){
    const size_t num_levels = attr_in->size();

    //allocate enough memory for each level
    uint8_t **level_memory = new uint8_t*[num_levels];
    for(size_t i = 0; i < num_levels; i++){
      std::cout << "level: " << i << " " << attr_in->at(i)->size() << std::endl;
      level_memory[i] = new uint8_t[attr_in->at(i)->size()*sizeof(uint32_t)];
    }

    //special code to encode the first level
    //std::multimap<char,int> mymultimap;
    std::vector<std::multimap<uint32_t,uint32_t>*> *mymultimaps = new std::vector<std::multimap<uint32_t,uint32_t>*>(); 
    size_t cur_level = 0;
    const Column<uint32_t> * const cur_attributes = attr_in->at(cur_level);
    const size_t num_attributes = cur_attributes->size();
    //parallel for
    std::multimap<uint32_t,uint32_t>* mymultimap = new std::multimap<uint32_t,uint32_t>(); 
    for(size_t i = 0; i < num_attributes; i++){
      std::cout << "attribute: " << i << std::endl;
      mymultimap->insert(std::pair<uint32_t,uint32_t>(cur_attributes->at(i),i));
    }
    cur_level++;
    mymultimaps->push_back(mymultimap);

    std::vector<Block*> *blocks = new std::vector<Block*>();
    blocks->push_back(new Block());



    encode(num_attributes,attr_in->at(cur_level),mymultimaps,blocks);
    //build set

    //for(size_t i = cur_level; i < num_levels; i++){}
    

    //for each thread you will need -> size of attribute memory (to build the set)
    //                              -> size of atribute  memory (to keep inidicies for next level)


    //pass a vector<vector<uint8_t*>> 3-levels down
    //the first level corresponds to the set
    //the second level corresponds to the array of indices
    //last level is the indices to look at (these will give us the set for the next level)
  
    //need a function that given a set of indices
    //pulls the values from input column, then appends next level info into a vector 
  }
  //for debug purposes
  void print();
};

#endif