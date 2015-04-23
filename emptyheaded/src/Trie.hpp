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

    while(cur == prev && (i+1) < end){
      prev = current->at(indicies[++i]);
    }

    next_ranges[num_distinct] = start_range;
    data[num_distinct] = cur;
    num_distinct++;
  }
  next_ranges[num_distinct] = end;
  return num_distinct; //should be all you need.
}

void encode_tail(size_t start, size_t end, uint32_t *data, Column<uint32_t> *current, uint32_t *indicies){
  for(size_t i = start; i < end; i++){
    *data++ = current->at(indicies[i]);
  }
}

template<typename F>
uint32_t* perform_selection(uint32_t *iterator, size_t num_rows, F f){
  for(size_t i = 0; i < num_rows; i++){
    if(f(i)){
      *iterator++ = i; 
    }
  }
  return iterator;
}

template<class T>
T* build_block(const size_t tid, allocator::memory<uint8_t> *data_allocator, 
  const size_t set_size, uint32_t *set_data_buffer){

  T *block = (T*)data_allocator->get_next(tid,sizeof(T));
  size_t set_alloc_size =  set_size*sizeof(uint64_t);
  uint8_t* set_data_in = data_allocator->get_next(tid,set_alloc_size,BYTES_PER_REG);
  block->data = Set<layout>::from_array(set_data_in,set_data_buffer,set_size);
  while(set_alloc_size < block->data.number_of_bytes){
    data_allocator->roll_back(tid,set_alloc_size);
    set_alloc_size *= 2;
    set_data_in = data_allocator->get_next(tid,set_alloc_size,BYTES_PER_REG);
    block->data = Set<layout>::from_array(set_data_in,set_data_buffer,set_size);
  }
  data_allocator->roll_back(tid,set_alloc_size-block->data.number_of_bytes);
  return block;
}

template<typename F>
inline Trie* Trie::build(std::vector<Column<uint32_t>> *attr_in, F f){
  const size_t num_levels = attr_in->size();
  const size_t num_rows = attr_in->at(0).size();

  //Filter rows via selection and sort for the Trie
  uint32_t *indicies = new uint32_t[num_rows];
  uint32_t *iterator = perform_selection(indicies,num_rows,f);
  const size_t num_rows_post_filter = iterator-indicies;
  tbb::task_scheduler_init init(NUM_THREADS);
  tbb::parallel_sort(indicies,iterator,SortColumns(attr_in));

  //Where all real data goes
  allocator::memory<uint8_t> data_allocator((num_rows*num_levels*(sizeof(uint64_t)+sizeof(Block)))/NUM_THREADS);
  //always just need two buffers(that swap)
  allocator::memory<size_t> ranges(num_rows_post_filter);
  allocator::memory<size_t> next_ranges(num_rows_post_filter);
  allocator::memory<uint32_t> set_data_buffer(num_rows_post_filter);
  allocator::memory<uint32_t> new_set_data_buffer(num_rows_post_filter);

  //Find the ranges for distinct values in the head
  size_t head_size = produce_ranges(0,
    num_rows_post_filter,
    ranges.get_memory(0),
    set_data_buffer.get_memory(0),
    indicies,
    &attr_in->at(0));

  //Build the head set.
  Head new_head = *build_block<Head>(0,&data_allocator,head_size,set_data_buffer.get_memory(0));
  new_head.map = (Block**)data_allocator.get_next(0,num_rows*sizeof(Block*));

  size_t cur_level = 1;
  par::for_range(0,head_size,100,[&](size_t tid, size_t i){
    (void) tid;
    //some sort of recursion here
    size_t start = ranges.get_memory(0)[i];
    size_t end = ranges.get_memory(0)[i+1];
    uint32_t data = set_data_buffer.get_memory(0)[i];
    
    while(cur_level < (num_levels-1)){
      std::cout << "ERROR" << std::endl;
      cur_level++;
    }

    //places items in set data buffer
    encode_tail(start,end,new_set_data_buffer.get_memory(tid),&attr_in->at(cur_level),indicies);

    Block *tail = build_block<Block>(tid,&data_allocator,(end-start),new_set_data_buffer.get_memory(tid));
    new_head.set_block(data,tail);
    /*
    std::cout << "node: " << data << std::endl;
    new_head.get_block(data)->data.foreach([&](uint32_t d){
      std::cout << d << std::endl;
    });
    */
  });

  next_ranges.deallocate();
  ranges.deallocate();
  set_data_buffer.deallocate();
  new_set_data_buffer.deallocate(); 

  //encode the set, create a block with NULL pointers to next level
  //should be a 1-1 between pointers in block and next ranges
  //also a 1-1 between blocks and numbers of next ranges

  return new Trie(new_head);
}
#endif