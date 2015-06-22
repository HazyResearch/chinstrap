#ifndef _TRIE_H_
#define _TRIE_H_

#include "TrieBlock.hpp"
#include "Encoding.hpp"
#include "tbb/parallel_sort.h"
#include "tbb/task_scheduler_init.h"

struct SortColumns{
  std::vector<Column<uint32_t>> *columns; 
  SortColumns(std::vector<Column<uint32_t>> *columns_in){
    columns = columns_in;
  }
  bool operator()(uint32_t i, uint32_t j) const {
    for(size_t c = 0; c < columns->size(); c++){
      if(columns->at(c).at(i) != columns->at(c).at(j)){
        return columns->at(c).at(i) < columns->at(c).at(j);
      }
    }
    return false;
  }
};

template<class T>
struct Trie{
  TrieBlock<T> *head;

  Trie<T>(TrieBlock<T> *head_in){
    head = head_in;
  };

  template<typename F>
  static Trie<T>* build(std::vector<Column<uint32_t>> *attr_in, F f);
};

size_t produce_ranges(size_t start, size_t end, 
  size_t *next_ranges, uint32_t *data,
  uint32_t *indicies, Column<uint32_t> * current){

  size_t num_distinct = 0;
  size_t i = start;
  while(true){
    const size_t start_range = i;
    const uint32_t cur = current->at(indicies[i]);
    uint32_t prev = cur;

    next_ranges[num_distinct] = start_range;
    data[num_distinct] = cur;

    ++num_distinct;

    while(cur == prev){
      if((i+1) >= end)
        goto FINISH;
      prev = current->at(indicies[++i]);
    }

  }
  FINISH:
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

template<class B, class T>
B* build_block(const size_t tid, allocator::memory<uint8_t> *data_allocator, 
  const size_t max_set_size, const size_t set_size, uint32_t *set_data_buffer){

  B *block = (B*)data_allocator->get_next(tid,sizeof(B),BYTES_PER_CACHELINE);
  const size_t set_alloc_size =  max_set_size*sizeof(uint64_t)+100;
  uint8_t* set_data_in = data_allocator->get_next(tid,set_alloc_size,BYTES_PER_REG);
  block->set = Set<T>::from_array(set_data_in,set_data_buffer,set_size);
    
  assert(set_alloc_size > block->set.number_of_bytes);
  data_allocator->roll_back(tid,set_alloc_size-block->set.number_of_bytes);
  return block;
}

template<class B,class T>
void recursive_build(const size_t index, const size_t start, const size_t end, const uint32_t data, B* prev_block, 
  const size_t level, const size_t num_levels, const size_t tid, 
  std::vector<Column<uint32_t>> *attr_in,
  allocator::memory<uint8_t> *data_allocator, const size_t num_rows, 
  std::vector<allocator::memory<size_t>> *ranges_buffer, 
  std::vector<allocator::memory<uint32_t>> *set_data_buffer, 
  uint32_t *indicies){
  
  uint32_t *sb = set_data_buffer->at(level).get_memory(tid);
  encode_tail(start,end,sb,&attr_in->at(level),indicies);

  B *tail = build_block<B,T>(tid,data_allocator,num_rows,(end-start),sb);
  prev_block->set_block(index,data,tail);

  if(level < (num_levels-1)){
    const size_t set_size = produce_ranges(start,end,ranges_buffer->at(level).get_memory(tid),set_data_buffer->at(level).get_memory(tid),indicies,&attr_in->at(level));
    for(size_t i = 0; i < set_size; i++){
      const size_t next_start = ranges_buffer->at(level).get_memory(tid)[i];
      const size_t next_end = ranges_buffer->at(level).get_memory(tid)[i+1];
      const uint32_t next_data = set_data_buffer->at(level).get_memory(tid)[i];        
      recursive_build<B,T>(i,next_start,next_end,next_data,tail,level+1,num_levels,tid,attr_in,data_allocator,num_rows,ranges_buffer,set_data_buffer,indicies);
    }
  }
}

template<class T> template <typename F>
inline Trie<T>* Trie<T>::build(std::vector<Column<uint32_t>> *attr_in, F f){
  const size_t num_levels = attr_in->size();
  const size_t num_rows = attr_in->at(0).size();

  //Filter rows via selection and sort for the Trie
  uint32_t *indicies = new uint32_t[num_rows];
  uint32_t *iterator = perform_selection(indicies,num_rows,f);
  const size_t num_rows_post_filter = iterator-indicies;

  tbb::task_scheduler_init init(NUM_THREADS);
  tbb::parallel_sort(indicies,iterator,SortColumns(attr_in));

  //Where all real data goes
  const size_t alloc_size = (8*num_rows*num_levels*sizeof(uint64_t)*sizeof(TrieBlock<T>))/NUM_THREADS;
  allocator::memory<uint8_t> data_allocator(alloc_size);
  //always just need two buffers(that swap)
  std::vector<allocator::memory<size_t>> *ranges_buffer = new std::vector<allocator::memory<size_t>>();
  std::vector<allocator::memory<uint32_t>> *set_data_buffer = new std::vector<allocator::memory<uint32_t>>();
  
  for(size_t i = 0; i < num_levels; i++){
    allocator::memory<size_t> ranges((num_rows_post_filter+1));
    allocator::memory<uint32_t> sd((num_rows_post_filter+1));
    ranges_buffer->push_back(ranges);
    set_data_buffer->push_back(sd); 
  }

  //Find the ranges for distinct values in the head
  const size_t head_size = produce_ranges(0,
    num_rows_post_filter,
    ranges_buffer->at(0).get_memory(0),
    set_data_buffer->at(0).get_memory(0),
    indicies,
    &attr_in->at(0));

  //Build the head set.
  TrieBlock<T>* new_head = build_block<TrieBlock<T>,T>(0,&data_allocator,num_rows,head_size,set_data_buffer->at(0).get_memory(0));
  new_head->is_sparse = false;
  new_head->next_level = (TrieBlock<T>**)data_allocator.get_next(0,num_rows*sizeof(TrieBlock<T>*));
  par::for_range(0,num_rows,100,[&](size_t tid, size_t i){
    (void) tid;
    new_head->next_level[i] = NULL;
  });

  size_t cur_level = 1;
  par::for_range(0,head_size,100,[&](size_t tid, size_t i){
    //some sort of recursion here
    const size_t start = ranges_buffer->at(0).get_memory(0)[i];
    const size_t end = ranges_buffer->at(0).get_memory(0)[i+1];
    const uint32_t data = set_data_buffer->at(0).get_memory(0)[i];    

    recursive_build<TrieBlock<T>,T>(i,start,end,data,new_head,cur_level,num_levels,tid,attr_in,
      &data_allocator,num_rows,ranges_buffer,set_data_buffer,indicies);

  });

  for(size_t i = 0; i < num_levels; i++){
    ranges_buffer->at(i).free();
    set_data_buffer->at(i).free();
  }

  //encode the set, create a block with NULL pointers to next level
  //should be a 1-1 between pointers in block and next ranges
  //also a 1-1 between blocks and numbers of next ranges

  return new Trie(new_head);
}
#endif