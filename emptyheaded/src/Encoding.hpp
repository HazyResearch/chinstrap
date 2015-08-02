/******************************************************************************
*
* Author: Christopher R. Aberger
*
* The encoding class performs the dictionary encoding and stores both
* a hash map from values to keys and an array from keys to values. Values
* here are the original values that appeared in the relations. Keys are the 
* distinct 32 bit integers assigned to each value.
******************************************************************************/

#ifndef _ENCODING_H_
#define _ENCODING_H_

#include <mutex>
#include <unordered_map>
#include "Column.hpp"

template <class T>
struct Encoding{
  std::unordered_map<T,uint32_t> value_to_key;
  std::vector<T> key_to_value;
  uint32_t num_distinct;

  Encoding(){
    num_distinct = 0;
  }

  //Given a column add its values to the encoding.
  uint32_t add_value(const T value){
    value_to_key.insert(std::make_pair(value,num_distinct));
    key_to_value.push_back(value);
    return ++num_distinct;
  }

  //Given a column add its values to the encoding.
  Column<uint32_t> add_column(Column<T> *attr_in){
    std::mutex mtx;
    Column<uint32_t> encoded_column;
    encoded_column.reserve(attr_in->size());
    par::for_range(0,attr_in->size(),50,[&](size_t tid, size_t i){
      (void) tid;
      const T value = attr_in->at(i);
      if(!value_to_key.count(value)){
        mtx.lock();
        add_element(value);
        mtx.unlock();
        encoded_column.set(i,value);
      }
    });
    return encoded_column;
  }
};

#endif