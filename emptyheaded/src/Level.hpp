#include "set/ops.hpp"

template <class T>
class Level{
  public:
  std::vector<T> *column;
  Level(const size_t alloc_size){
    column = new std::vector<T>();
    column->reserve(alloc_size);
  }

  void add(const char *string_element);
};