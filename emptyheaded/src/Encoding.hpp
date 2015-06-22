#ifndef _ENCODING_H_
#define _ENCODING_H_

#include <unordered_map>
#include "Column.hpp"
#include "tbb/parallel_sort.h"
#include "tbb/task_scheduler_init.h"

template<class T>
struct OrderByFrequency{
  std::vector<size_t> *frequency;
  OrderByFrequency(std::vector<size_t> *frequency_in){
    frequency = frequency_in;
  }
  bool operator()(std::pair<uint32_t,T> i, std::pair<uint32_t,T> j) const {
    size_t i_size = frequency->at(i.first);
    size_t j_size = frequency->at(j.first);
    return i_size > j_size;
  }
};

template <class T>
struct Encoding{
  std::vector<Column<uint32_t>> encoded;
  std::unordered_map<T,uint32_t> value_to_key;
  std::vector<T> key_to_value;
  size_t num_distinct;

  Encoding(std::vector<Column<T>> *attr_in){
    const size_t num_attributes = attr_in->size();
    const size_t num_rows = attr_in->at(0).size();

    //auto a = debug::start_clock();
    
    //Get the frequency of each value
    std::vector<std::pair<uint32_t,T>> sorted_values;
    std::vector<size_t> frequency;

    sorted_values.reserve(num_rows*num_attributes);
    frequency.reserve(num_rows*num_attributes);
    value_to_key.reserve(num_rows*num_attributes);

    //TODO: Parallelize, cleanup memory usage.
    for(size_t i = 0; i < num_attributes; i++){
      const Column<T> input = attr_in->at(i);
      for(size_t j = 0; j < input.size(); j++){
        const T value = input.at(j);
        if(!value_to_key.count(value)){
          value_to_key.insert(std::make_pair(value,sorted_values.size()));
          sorted_values.push_back(std::make_pair(sorted_values.size(),value));
          frequency.push_back(1);
        } else{
          frequency.at(value_to_key.at(value))++;
        }
      }
    }
    //debug::stop_clock("serial",a);

    //a = debug::start_clock();
    //sort by the frequency
    tbb::task_scheduler_init init(NUM_THREADS);
    tbb::parallel_sort(sorted_values.begin(),sorted_values.end(),OrderByFrequency<T>(&frequency));

    //reassign id's

    T *k2v = new T[sorted_values.size()];
    key_to_value.assign(k2v,k2v+sorted_values.size());
    par::for_range(0,sorted_values.size(),100,[&](size_t tid, size_t i){
      (void) tid;
      key_to_value.at(i) = sorted_values.at(i).second;
      value_to_key.at(sorted_values.at(i).second) = i;
    });

    //create new encoded column
    encoded.reserve(num_attributes);

    const size_t alloc_size = num_attributes*num_rows;
    uint32_t *new_columns = new uint32_t[alloc_size];

    for(size_t i = 0; i < num_attributes; i++){
      const Column<T> input = attr_in->at(i);
      Column<uint32_t> output;
      output.assign(new_columns+(i*num_rows),new_columns+((i+1)*num_rows));
      par::for_range(0,input.size(),100,[&](size_t tid, size_t j){
        (void) tid;
        const T value = input.at(j);
        const uint32_t key = value_to_key.at(value);
        output.set(j,key);
      });
      encoded.push_back(output);
    }
    //debug::stop_clock("parallel",a);

    num_distinct = key_to_value.size();
  };
};

#endif