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
      for(size_t i = 0; i < num_elems; i++){
        a.construct(data+i);
      }
      index = 0;
    }
    T* get_next(size_t num){
      T* val = &data[index];
      index += num;
      return val;
    }
    T* get_next(){
      return &data[index++];
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
    T* get_next(size_t tid){
      return elements->at(tid).get_next();
    }
    T* get_next(size_t tid, size_t num){
      return elements->at(tid).get_next(num);
    }
  };
};
#endif
