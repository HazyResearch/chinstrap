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
#include <unordered_set>
#include <map>
#include "tbb/parallel_sort.h"

///////////////////////////////////////////////////////////////////////////////
/*
  Have class so we can do arbitrary types
  of sortings for the dictionary encoding. By frequency
  or whatever you should always be able to write your own
  class to specify how we order for the dictionary encoding.
*/

//Basic encoding map where you sort by the sortable type itself.
template <class T>
struct SortableEncodingMap{
  std::set<T> values;

  SortableEncodingMap(){}
  ~SortableEncodingMap(){}

  //Have a way to add values
  inline void update(const T value){
    values.insert(value);
  }
  //Have a way to iterate over values (in the way you want)
  template<typename F>
  inline void foreach(const F f){
    for (auto it = values.begin(); it != values.end(); it++) {
      f(*it);
    }
  }
};

//Encoding map where you sort based upon the frequency of the values
template <class T>
struct FrequencyEncodingMap{
  std::map<T,uint32_t> values;

  FrequencyEncodingMap(){}
  ~FrequencyEncodingMap(){
    delete values;
  }

  //Have a way to add values
  inline void update(const T value){
    const auto it = std::find(value);
    uint32_t freq = 1;
    if(it != values.end()){
      freq = it->second + 1;
      values.erase(it);
    }
    values.insert(std::pair<T,uint32_t>(value,freq));
  }

  //sort functor (sort by value not key)
  bool myFunction(std::pair<T,uint32_t> first, std::pair<T,uint32_t> second){
    return first.second < second.second;
  }

  //Have a way to iterate over values (in the way you want)
  template<typename F>
  inline void foreach(const F f){
    //sort by the frequency
    
    //first create a vector with contents of the map, then sort that vector
    std::vector<std::pair<T,uint32_t> > myVec(values.begin(), values.end());
    tbb::parallel_sort(myVec.begin(),myVec.end(),&myFunction);

    for(auto it = myVec.begin(); it != myVec.end(); it++) {
      f(*it);
    }
    delete myVec;
  }
};
///////////////////////////////////////////////////////////////////////////////
template <class T>
struct Encoding{
  std::unordered_map<T,uint32_t> value_to_key;
  std::vector<T> key_to_value;
  uint32_t num_distinct;
  size_t disk_index;

  Encoding(){
    num_distinct = 0;
    disk_index = 0;
  }
  ~Encoding(){}

  //Given a column add its values to the encoding.
  template <class K>
  void build(K* encodingMap){
    uint32_t index = 0;
    encodingMap->foreach([&](T value){
      value_to_key.insert(std::pair<T,uint32_t>(value,index++));
      key_to_value.push_back(value);
    });
  }

  std::vector<uint32_t>* encode_column(std::vector<T>* encodingMap){
    std::vector<uint32_t>* column = new std::vector<uint32_t>();
    column->resize(encodingMap->size());
    par::for_range(0,encodingMap->size(),100,[&](size_t tid, size_t i){
      (void) tid;
      column->at(i) = value_to_key.at(encodingMap->at(i));
    });
    return column;
  }
};

#endif