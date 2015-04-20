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
    inline T* get_next(size_t num){
      if(index + num < max_index){
        T* val = &data[index];
        index += num;
        return val;
      }
      return NULL;
    }
    inline T* get_next(){
      if(index < max_index)
        return &data[index++];
      return NULL;
    }
  };

  template<class T>
  class memory{
  public:
    const size_t multplier = 2;
    size_t num_elems;
    std::vector<size_t> indicies;
    std::vector<std::vector<elem<T>>> *elements;
    memory(size_t num_elems_in){
      num_elems = num_elems_in;
      elements = new std::vector<std::vector<elem<T>>>();
      for(size_t i = 0; i < NUM_THREADS; i++){
        std::vector<elem<T>> current;
        current.push_back(elem<T>(num_elems));
        elements->push_back(current);
        indicies.push_back(0);
      }
    }
    inline T* get_memory(size_t tid){
      return elements->at(tid).at(indicies.at(tid)).data;
    }
    inline T* get_next(size_t tid){
      T* val = elements->at(tid).at(indicies.at(tid)).get_next();
      if(val == NULL){
        elements->at(tid).push_back(elem<T>(num_elems));
        indicies.at(tid)++;
        val = elements->at(tid).at(indicies.at(tid)).get_next();
        assert(val != NULL);
      }
      return val;
    }
    inline T* get_next(size_t tid, size_t num){
      T* val = elements->at(tid).at(indicies.at(tid)).get_next(num);
      if(val == NULL){
        while(num > num_elems)
          num_elems = num_elems*multplier;

        std::cout << "Reallocing" << std::endl;
        assert(num < num_elems);
        elements->at(tid).push_back(elem<T>(num_elems));
        indicies.at(tid)++;
        val = elements->at(tid).at(indicies.at(tid)).get_next(num);
        assert(val != NULL);
      }
      return val;
    }
  };
};
#endif
