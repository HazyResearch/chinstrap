#ifndef allocator_H
#define allocator_H

#include "common.hpp"

namespace allocator{
  template<class T>
  class elem{
  public:
    size_t index;
    size_t max_index;
    std::allocator<T> a;
    T* data;
    elem(size_t num_elems){
      max_index = num_elems;
      data = a.allocate(num_elems);
      index = 0;
    }
  };

  template<class T>
  class memory{
  public:
    std::vector<elem<T>> *elements;
    memory(size_t num_elems){
      elements = new std::vector<elem<T>>();
      for(size_t i = 0; i < NUM_THREADS; i++){
        elements->push_back(elem<T>(num_elems));
      }
    }
    T* get_memory(size_t tid){
      return elements->at(tid).data;
    }

  };
};
#endif
