#ifndef PARALLEL_H
#define PARALLEL_H

#include "thread_pool.hpp"

namespace par{
  template<class T>
  class reducer{
  public:
    size_t padding = 300;
    T *elem;
    //tbb::cache_aligned_allocator<T>
    std::function<T (T, T)> f;
    reducer(T init_in, std::function<T (T, T)> f_in){
      f = f_in;
      elem = new T[NUM_THREADS*padding];
      for(size_t i = 0; i < NUM_THREADS; i++){
        elem[i*padding] = init_in;
      }
      memset(elem,(uint8_t)0,sizeof(T)*NUM_THREADS*padding);
    }
    inline void update(size_t tid, T new_val){
      elem[tid*padding] = f(elem[tid*padding],new_val);
    }
    inline T evaluate(T init){
      for(size_t i = 0; i < NUM_THREADS; i++){
        init = f(init,elem[i*padding]);
      }
      return init;
    }
  };


  struct parFor {
    size_t tid;
    static std::atomic<size_t> next_work;
    static size_t block_size;
    static size_t range_len;
    static size_t offset;
    static std::function<void(size_t, size_t)> body;
    parFor(size_t tid_in){
      tid = tid_in;
    }
    void* run();
  };

  template<typename F>
  size_t for_range(const size_t from, const size_t to, const size_t block_size, F body) {
    const size_t range_len = to - from;
    
    thread_pool::init_threads();
    parFor::next_work = 0;
    parFor::block_size = block_size;
    parFor::range_len = range_len;
    parFor::offset = from;
    parFor::body = body;
    for(size_t k = 0; k < NUM_THREADS; k++) { 
      parFor* pf = new parFor(k);
      thread_pool::submitWork(k,thread_pool::general_body<parFor>,(void *)(pf));
    }
    thread_pool::join_threads();
    return 1;
  }
}
#endif
