#include "set/ops.hpp"

template <class T>
class Attribute{
  public:
  std::vector<T> *column;
  Attribute(const size_t alloc_size){
    column = new std::vector<T>();
    column->reserve(alloc_size);
  }

  void add(const char *string_element);
};

template<>
inline void Attribute<uint64_t>::add(const char *string_element){
  uint64_t element;
  sscanf(string_element,"%lu",&element);
  column->push_back(element);
}

template<>
inline void Attribute<uint32_t>::add(const char *string_element){
  uint32_t element;
  sscanf(string_element,"%u",&element);
  column->push_back(element);
}

template<>
inline void Attribute<std::string>::add(const char *string_element){
  column->push_back(string_element);
}