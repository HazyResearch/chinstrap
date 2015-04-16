#ifndef _COLUMN_H_
#define _COLUMN_H_

#include "utils/utils.hpp"

template <class T>
struct Column{
  std::vector<T> col;
  Column<T>(){}
  inline T at(size_t i) const{
    return col.at(i);
  }
  inline void set(size_t i, T val){
    col.at(i) = val;
  }
  inline void append(T elem){
    col.push_back(elem);
  }
  inline size_t size() const{
    return col.size();
  }
  inline void reserve(size_t size){
    col.reserve(size);
  }
  inline void assign(T * start, T* end){
    col.assign(start,end);
  }

  void append_from_string(const char *string_element);
};

template<>
inline void Column<uint64_t>::append_from_string(const char *string_element){
  uint64_t element;
  sscanf(string_element,"%lu",&element);
  col.push_back(element);
}

template<>
inline void Column<uint32_t>::append_from_string(const char *string_element){
  uint32_t element;
  sscanf(string_element,"%u",&element);
  col.push_back(element);
}

template<>
inline void Column<std::string>::append_from_string(const char *string_element){
  col.push_back(string_element);
}

#endif