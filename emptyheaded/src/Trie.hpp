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
  Head head;

  Trie(Head head_in){
    head = head_in;
  };

  template<typename F>
  static Trie* build(std::vector<Column<uint32_t>> *attr_in, F f);
};

size_t produce_ranges(size_t start, size_t end, 
  size_t *next_ranges, uint32_t *data,
  uint32_t *indicies, Column<uint32_t> * current){

  size_t num_distinct = 0;
  size_t i = start;
  while((i+1) < end){
    size_t start_range = i;
    uint32_t cur = current->at(indicies[i]);
    uint32_t prev = cur;

    //std::cout << cur << std::endl;
    while(cur == prev && (i+1) < end){
      prev = current->at(indicies[++i]);
    }

    next_ranges[num_distinct] = start_range;
    //std::cout << "Elem #: " << num_distinct << " " << start_range << std::endl;
    data[num_distinct] = cur;
    num_distinct++;
  }
  next_ranges[num_distinct] = end;
  //std::cout << "Final: " << num_distinct << std::endl;
  return num_distinct; //should be all you need.
}

void encode_tail(size_t start, size_t end, uint32_t *data, Column<uint32_t> *current, uint32_t *indicies){
  //std::cout << indicies[3] << std::endl;
  for(size_t i = start; i < end; i++){
    *data++ = current->at(indicies[i]);
  }
}

template<typename F>
inline Trie* Trie::build(std::vector<Column<uint32_t>> *attr_in, F f){
  const size_t num_levels = attr_in->size();
  const size_t num_rows = attr_in->at(0).size();

  uint32_t *indicies = new uint32_t[num_rows];
  uint32_t *iterator = indicies;
  for(size_t i = 0; i < num_rows; i++){
    if(f(i)){
      *iterator++ = i; 
    }
  }
  tbb::task_scheduler_init init(NUM_THREADS);
  tbb::parallel_sort(indicies,iterator,SortColumns(attr_in));

  //always just need two buffers
  const size_t num_rows_post_filter = iterator-indicies;

  allocator::memory<uint8_t> data_allocator(num_rows*num_levels*sizeof(uint64_t)+sizeof(Block));
  size_t *ranges = new size_t[num_rows_post_filter];
  size_t *next_ranges = new size_t[num_rows_post_filter];
  uint32_t *set_data_buffer = new uint32_t[num_rows_post_filter];
  uint32_t *new_set_data_buffer = new uint32_t[num_rows_post_filter];

  //std::cout << "stage 1" << std::endl;

  ///std::pair<size_t,size_t> 
  size_t head_size = produce_ranges(0,
    num_rows_post_filter,
    ranges,
    set_data_buffer,
    indicies,
    &attr_in->at(0));

  std::cout << head_size << std::endl;

  Head new_head;
  uint8_t* set_data_in = data_allocator.get_next(0,head_size*sizeof(uint64_t));
  new_head.data = Set<uinteger>::from_array(set_data_in,set_data_buffer,head_size);

  /*  
  new_head.data.foreach([&](uint32_t d){
    std::cout << "data: " << d << " " << set_data_buffer[d] << std::endl;
  });
  */
  
  new_head.map = (Block*)data_allocator.get_next(0,num_rows*sizeof(Block));

  //memset to null
  /*
  par::for_range(0,num_rows,100,[&](size_t tid, size_t i){
    (void) tid;
    new_head.set_block(i,NULL);
  });
  */

  std::cout << "made it to for" << std::endl;

  size_t cur_level = 1;
  par::for_range(0,head_size,100,[&](size_t tid, size_t i){
    (void) tid;
    //some sort of recursion here
    size_t start = ranges[i];
    size_t end = ranges[i+1];
    uint32_t data = set_data_buffer[i];
    
    while(cur_level < (num_levels-1)){
      std::cout << "ERROR" << std::endl;
      cur_level++;
    }

    //places items in set data buffer
    encode_tail(start,end,new_set_data_buffer,&attr_in->at(cur_level),indicies);

    Block tail;// = new Block();
    uint8_t* set_data_in = data_allocator.get_next(0,(end-start)*sizeof(uint64_t));
    tail.data = Set<uinteger>::from_array(set_data_in,new_set_data_buffer,(end-start));

    new_head.set_block(data,tail);
    
    /*
    std::cout << "Node: " << data << " " << set_data_buffer[1] << std::endl;
    new_head.get_block(data).data.foreach([&](uint32_t data){
      std::cout << "\tnbr: " << data << std::endl;
    });
    */

  });


  //encode the set, create a block with NULL pointers to next level
  //should be a 1-1 between pointers in block and next ranges
  //also a 1-1 between blocks and numbers of next ranges

  return new Trie(new_head);
}
#endif