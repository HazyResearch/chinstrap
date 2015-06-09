#ifndef PARALLEL_H
#define PARALLEL_H

#include "debug.hpp"
#include <thread>
#include <atomic>
#include <cstring>

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

  static std::thread* threads = NULL;
  static void init_threads() {
    threads = new std::thread[NUM_THREADS];
  }

  // Iterates over a range of numbers in parallel
  template<typename F>
  static size_t for_range(const size_t from, const size_t to, const size_t block_size, F body) {
     const size_t range_len = to - from;
     //std::cout << range_len << " " << block_size << std::endl;
     const size_t real_num_threads = std::min(range_len / block_size + 1, NUM_THREADS);
     //std::cout << "Range length: " << range_len << " Threads: " << real_num_threads << std::endl;

     if(real_num_threads == 1) {
        for(size_t i = from; i < to; i++) {
           body(0, i);
        }
     }
     else {
        auto t_begin = debug::start_clock();
        double* thread_times = NULL;

#ifdef ENABLE_PRINT_THREAD_TIMES
        thread_times = new double[real_num_threads];
#endif
        std::thread* threads = new std::thread[real_num_threads];
        const size_t range_len = to - from;
        std::atomic<size_t> next_work;
        next_work = 0;

        for(size_t k = 0; k < real_num_threads; k++) {
           threads[k] = std::thread([&block_size](std::chrono::time_point<std::chrono::system_clock> t_begin, double* thread_times, int k, std::atomic<size_t>* next_work, size_t offset, size_t range_len, std::function<void(size_t, size_t)> body) -> void {
              size_t local_block_size = block_size;

              while(true) {
                 size_t work_start = next_work->fetch_add(local_block_size, std::memory_order_relaxed);
                 if(work_start > range_len)
                    break;

                 size_t work_end = std::min(work_start + local_block_size, range_len);
                 local_block_size = block_size;//100 + (work_start / range_len) * block_size;
                 for(size_t j = work_start; j < work_end; j++) {
                     body(k, offset + j);
                 }
              }


#ifdef ENABLE_PRINT_THREAD_TIMES
              thread_times[k] = debug::stop_clock(t_begin);
#else
              (void) t_begin;
              (void) thread_times;
#endif
           }, t_begin, thread_times, k, &next_work, from, range_len, body);
        }

        for(size_t k = 0; k < real_num_threads; k++) {
           threads[k].join();
        }

#ifdef ENABLE_PRINT_THREAD_TIMES
        for(size_t k = 0; k < real_num_threads; k++){
            std::cout << "Execution time of thread " << k << ": " << thread_times[k] << std::endl;
        }
        delete[] thread_times;
#endif
     }

     return real_num_threads;
  }
  static size_t for_range(const size_t from, const size_t to, const size_t block_size,
    std::function<void(size_t)> setup,
    std::function<void(size_t, size_t)> body,
    std::function<void(size_t)> tear_down) {

    #ifdef ENABLE_PRINT_THREAD_TIMES
    double setup1 = debug::start_clock();
    #endif

    for(size_t i = 0; i < NUM_THREADS; i++){
      setup(i);
    }

    #ifdef ENABLE_PRINT_THREAD_TIMES
    debug::stop_clock("PARALLEL SETUP",setup1);
    #endif

    size_t real_num_threads = for_range(from,to,block_size,body);

    #ifdef ENABLE_PRINT_THREAD_TIMES
    double td = debug::start_clock();
    #endif

    for(size_t i = 0; i < NUM_THREADS; i++){
      tear_down(i);
    }

    #ifdef ENABLE_PRINT_THREAD_TIMES
    debug::stop_clock("PARALLEL TEAR DOWN",td);
    #endif

    return real_num_threads;
  }

  static std::vector<uint32_t> range(uint32_t max) {
    std::vector<uint32_t> result;
    result.reserve(max);
    for(uint32_t i = 0; i < max; i++)
      result.push_back(i);
    return result;
  }
}
#endif
