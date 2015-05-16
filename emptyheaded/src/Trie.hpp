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
  bool operator()(size_t i, size_t j) const {
    for(size_t c = 0; c < columns->size(); c++){
      if(columns->at(c).at(i) != columns->at(c).at(j)){
        return columns->at(c).at(i) < columns->at(c).at(j);
      }
    }
    return true;
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

    if(++i == end)
      break;
    num_distinct++;
    while(cur == prev && i < end){
      prev = current->at(indicies[i++]);
    }
    --i;
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
  allocator::memory<uint8_t> data_allocator((num_rows*num_levels*sizeof(uint64_t)*sizeof(TrieBlock<T>))/NUM_THREADS);
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
  TrieBlock<T>* new_head = build_block<TrieBlock<T>,T>(0,&data_allocator,num_rows,head_size,set_data_buffer.get_memory(0));
  new_head->is_sparse = false;
  new_head->next_level = (TrieBlock<T>**)data_allocator.get_next(0,num_rows*sizeof(TrieBlock<T>*));
  par::for_range(0,num_rows,100,[&](size_t tid, size_t i){
    (void) tid;
    new_head->next_level[i] = NULL;
  });

  size_t cur_level = 1;
  par::for_range(0,head_size,100,[&](size_t tid, size_t i){
    //some sort of recursion here
    const size_t start = ranges.get_memory(0)[i];
    const size_t end = ranges.get_memory(0)[i+1];
    const uint32_t data = set_data_buffer.get_memory(0)[i];    

    //std::cout << "s: " << start << " e: " << end << std::endl;

    while(cur_level < (num_levels-1)){
      std::cout << "ERROR" << std::endl;
      cur_level++;
    }

    //places items in set data buffer
    uint32_t *sb = new_set_data_buffer.get_memory(tid);
    encode_tail(start,end,sb,&attr_in->at(cur_level),indicies);

    TrieBlock<T> *tail = build_block<TrieBlock<T>,T>(tid,&data_allocator,num_rows,(end-start),sb);
    new_head->set_block(i,data,tail);
    /*
    std::cout << "node: " << data << std::endl;
    new_head.get_block(data)->data.foreach([&](uint32_t d){
      std::cout << d << std::endl;
    });
    */

  });

  next_ranges.free();
  ranges.free();
  set_data_buffer.free();
  new_set_data_buffer.free(); 

  //encode the set, create a block with NULL pointers to next level
  //should be a 1-1 between pointers in block and next ranges
  //also a 1-1 between blocks and numbers of next ranges

  return new Trie(new_head);
}
#endif