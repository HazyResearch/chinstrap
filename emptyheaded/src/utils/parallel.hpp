#ifndef PARALLEL_H
#define PARALLEL_H

#include "debug.hpp"
#include <thread>
#include <atomic>
#include <cstring>
#include <pthread.h>
#include <sched.h>

//Inspired (read copied) from Delite
namespace thread_pool {
  pthread_t* threadPool;
  pthread_mutex_t* locks;
  pthread_cond_t* readyConds;
  pthread_cond_t* doneConds;
  std::atomic<size_t> init_barrier; // barrier synchronization object

  void** workPool;
  void** argPool;

  void initializeThread(size_t threadId) {
    #ifdef __linux__
      cpu_set_t cpu;
      CPU_ZERO(&cpu);
      CPU_SET(threadId, &cpu);
      sched_setaffinity(0, sizeof(cpu_set_t), &cpu);

      #ifdef __DELITE_CPP_NUMA__
        if (numa_available() >= 0) {
          int socketId = config->threadToSocket(threadId);
          if (socketId < numa_num_configured_nodes()) {
            bitmask* nodemask = numa_allocate_nodemask();
            numa_bitmask_setbit(nodemask, socketId);
            numa_set_membind(nodemask);
          }
          //VERBOSE("Binding thread %d to cpu %d, socket %d\n", threadId, threadId, socketId);
        }
      #endif
    #endif

    #ifdef __sun
      processor_bind(P_LWPID, P_MYID, threadId, NULL);
    #endif
  }

  void submitWork(size_t threadId, void *(*work) (void *), void *arg) {
    pthread_mutex_lock(&locks[threadId]);
    while (argPool[threadId] != NULL) {
      pthread_cond_wait(&doneConds[threadId], &locks[threadId]);
    }
    workPool[threadId] = (void*)work;
    argPool[threadId] = arg;
    pthread_cond_signal(&readyConds[threadId]);
    pthread_mutex_unlock(&locks[threadId]);
  }

  void* processWork(void* threadId) {
    size_t id = (size_t)threadId;

    /////////////////////////////////////////////////
    //Per Thead Initialization code
    //VERBOSE("Initialized thread with id %d\n", id);
    pthread_mutex_init(&locks[id], NULL);
    pthread_cond_init(&readyConds[id], NULL);
    pthread_cond_init(&doneConds[id], NULL);
    workPool[id] = NULL;
    argPool[id] = NULL;
    initializeThread(id);
    void *(*work) (void *);
    void *arg;
    /////////////////////////////////////////////////

    //don't use a barrier here because we don't want to block.
    //tell the init code that the thread is alive and rocking
    init_barrier.fetch_add(1);
    while(true) {
      pthread_mutex_lock(&locks[id]);
      while (argPool[id] == NULL) {
        pthread_cond_wait(&readyConds[id], &locks[id]);
      }      
      work = (void *(*)(void *))workPool[id];
      workPool[id] = NULL;
      arg = argPool[id];
      argPool[id] = NULL;
      pthread_cond_signal(&doneConds[id]);
      pthread_mutex_unlock(&locks[id]);
      work(arg);
    }
  }

  void initializeThreadPool() {
    threadPool = new pthread_t[NUM_THREADS];
    locks = new pthread_mutex_t[NUM_THREADS];
    readyConds = new pthread_cond_t[NUM_THREADS];
    doneConds = new pthread_cond_t[NUM_THREADS];
    workPool = new void*[NUM_THREADS];
    argPool = new void*[NUM_THREADS];

    init_barrier = 0;
    for (size_t i=0; i<NUM_THREADS; i++) {
      std::cout << "CREATING: " << i << std::endl; 
      pthread_create(&threadPool[i], NULL, processWork, (void*)i);
    }
    while(init_barrier.load() != NUM_THREADS){}
  }
  void* killThread(void *args_in){
    (void) args_in;
    pthread_exit(NULL);
  }
  void deleteThreadPool() {
    for(size_t k = 0; k < NUM_THREADS; k++) { 
      submitWork(k,killThread,(void *)NULL);
    }
  }
}

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

  pthread_barrier_t barrier; // barrier synchronization object

  // Iterates over a range of numbers in parallel
  void* for_body(void *args_in){
    (void) args_in;
    std::cout << "THREAD BODY "  << std::endl;
    pthread_barrier_wait (&barrier); 
    return NULL;
  }

  struct parFor {
    size_t block_size;
    parFor(size_t block_size_in){
      block_size = block_size_in;
    }
  };

  template<typename F>
  void for_range(const size_t from, const size_t to, const size_t block_size, F body) {
    const size_t range_len = to - from;
    parFor pf(block_size);

    pthread_barrier_init (&barrier, NULL, NUM_THREADS+1);
    for(size_t k = 0; k < NUM_THREADS; k++) { 
      thread_pool::submitWork(k,for_body,(void *)(&pf));
    }
    pthread_barrier_wait (&barrier);
  }
}
#endif
