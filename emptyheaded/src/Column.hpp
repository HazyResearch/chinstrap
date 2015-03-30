#ifndef _COLUMN_H_
#define _COLUMN_H_

#include "set/ops.hpp"

template <class T>
class Column{
  std::vector<T>* col;

public:
  Column<T>(){
    col = new std::vector<T>();
  }
  inline T at(size_t i){
    return col->at(i);
  };
  inline void append(T elem){
    col->push_back(elem);
  }
  inline void reserve(size_t size){
    col->reserve(size);
  }
  void append_from_string(const char *string_element);
};

template<>
inline void Column<uint64_t>::append_from_string(const char *string_element){
  uint64_t element;
  sscanf(string_element,"%lu",&element);
  col->push_back(element);
}

template<>
inline void Column<uint32_t>::append_from_string(const char *string_element){
  uint32_t element;
  sscanf(string_element,"%u",&element);
  col->push_back(element);
}

template<>
inline void Column<std::string>::append_from_string(const char *string_element){
  col->push_back(string_element);
}

#endif