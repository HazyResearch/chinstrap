#ifndef _TRIE_H_
#define _TRIE_H_

#include "Block.hpp"
#include "Encoding.hpp"
#include "tbb/parallel_sort.h"
#include "tbb/task_scheduler_init.h"

struct SortColumns{
  std::vector<Column<uint32_t>> *columns; 
  SortColumns(std::vector<Column<uint32_t>> *columns_in){
    columns = columns_in;
  }
  bool operator()(size_t i, size_t j) const {
    for(size_t c = 0; c < columns->size(); c++){
      if(columns->at(c).at(i) != columns->at(c).at(j)){
        return columns->at(c).at(i) < columns->at(c).at(j);
      }
    }
    return true;
  }
};


struct Trie{
  template<typename F>
  static Trie* build(std::vector<Column<uint32_t>> *attr_in, F f);
};
/*
std::pair<std::vector<std::multimap<uint32_t,uint32_t>*>*,std::vector<SparseBlock<SparseBlock*>*>*> encode(
  TrieAllocator *my_allocator,
  const size_t max_size,
  Column<uint32_t> *cur_column,
  std::vector<std::multimap<uint32_t,uint32_t>*> *mymultimaps, 
  std::vector<SparseBlock<SparseBlock*>*> *blocks){
  //encode sets out of multimaps
  //build hash maps

  std::vector<std::multimap<uint32_t,uint32_t>*> *outputmultimaps = new std::vector<std::multimap<uint32_t,uint32_t>*>();
  std::vector<SparseBlock<SparseBlock*>*> *outblocks = new std::vector<SparseBlock<SparseBlock*>*>();
  //maybe use a vector of blocks instead of a vector of block pointers
  uint32_t *new_set = new uint32_t[max_size];
  for(size_t i = 0; i < mymultimaps->size(); i++){
    int prev = -1;
    std::multimap<uint32_t,uint32_t> *mymultimap = mymultimaps->at(i);

    std::multimap<uint32_t,uint32_t>*outmap = NULL;
    SparseBlock<SparseBlock*> *outblock = NULL;
    size_t new_set_size = 0;

    for (auto it=mymultimap->begin(); it!=mymultimap->end(); ++it){
      if((int)(*it).first != prev){
        if(outblock != NULL && outmap != NULL){
          outputmultimaps->push_back(outmap);
          outblocks->push_back(outblock);
        }
        outblock = my_allocator->block_allocator->get_next(0);

        outmap = my_allocator->multimap_allocator->get_next(0);
        prev = (*it).first;
        new_set[new_set_size++] = (*it).first;
        blocks->at(i)->map.insert(std::pair<uint32_t,SparseBlock<SparseBlock*>*>((*it).first,outblock));
      }
      if(cur_column != NULL){
        outmap->insert(std::pair<uint32_t,uint32_t>(cur_column->at((*it).second),(*it).second));
      }
    }

    outputmultimaps->push_back(outmap);
    outblocks->push_back(outblock);

    //set is built now add it to the block
    uint8_t *set_data_in = my_allocator->data_allocator->get_next(0,new_set_size*sizeof(uint64_t));
    blocks->at(i)->data = Set<uinteger>::from_array(set_data_in,new_set,new_set_size);
  }

  return make_pair(outputmultimaps,outblocks);
}
*/

template<typename F>
inline Trie* Trie::build(std::vector<Column<uint32_t>> *attr_in, F f){

  const size_t num_levels = attr_in->size();
  const size_t num_rows = attr_in->at(0).size();

  uint32_t *indicies = new uint32_t[num_rows];
  uint32_t *iterator = indicies;
  for(size_t i = 0; i < num_rows; i++){
    *iterator++ = i; 
  }
  tbb::task_scheduler_init init(NUM_THREADS);
  tbb::parallel_sort(indicies,indicies+num_rows,SortColumns(attr_in));

  /*
  for(size_t i = 0; i < num_rows; i++){
    std::cout << indicies[i] << std::endl;
  }
  */

/*
  //special code to encode the first level
  std::vector<std::multimap<uint32_t,uint32_t>*> *mymultimaps = new std::vector<std::multimap<uint32_t,uint32_t>*>(); 
  size_t cur_level = 0;
  const Column<uint32_t> cur_attributes = attr_in->at(cur_level);
  const size_t num_attributes = cur_attributes.size();
  */
/*
  auto a0 = debug::start_clock();
  //parallel for
  std::multimap<uint32_t,uint32_t>* mymultimap = my_allocator->multimap_allocator->get_next(0); 
  for(size_t i = 0; i < num_attributes; i++){
    if(f(i))
      mymultimap->insert(std::pair<uint32_t,uint32_t>(cur_attributes.at(i),i));
  }
  cur_level++;
  mymultimaps->push_back(mymultimap);

  std::vector<SparseBlock<SparseBlock*>*> *blocks = new std::vector<SparseBlock<SparseBlock*>*>();
  
  */
  // = new Block<Sparse<Block<Sparse>>>();
  //Block<SparseBlock> sb = new Block<SparseBlock>();
//my_allocator->block_allocator->get_next(0);
  /*
  blocks->push_back(head);

  //////////////////////////////////////////////////////////////////////////////////////////////////////
  //Encode the remaining levels
  typedef std::pair<std::vector<std::multimap<uint32_t,uint32_t>*>*,std::vector<SparseBlock<SparseBlock*>*>*> level_pair;
  for(; cur_level < num_levels; cur_level++){
    level_pair out_pair = encode(my_allocator,num_attributes,&attr_in->at(cur_level),mymultimaps,blocks);
    mymultimaps = out_pair.first;
    blocks = out_pair.second;
  }
  encode(my_allocator,num_attributes,NULL,mymultimaps,blocks);

  debug::stop_clock("building without allocs",a0);
  */
  return new Trie();
}
#endif