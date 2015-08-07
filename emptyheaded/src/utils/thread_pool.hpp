#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include "debug.hpp"
#include <thread>
#include <atomic>
#include <cstring>
#include <pthread.h>
#include <sched.h>

namespace thread_pool {
  ////////////////////////////////////////////////////
  //Main thread should always call (init_threads, submit work
  // with the general body, then join threads.
  ////////////////////////////////////////////////////
  pthread_barrier_t barrier; // barrier synchronization object
  // Iterates over a range of numbers in parallel

  //general body is submitted to submit_work. Construct you op in the object
  //that is passed in as an arg.
  template<class F>
  void* general_body(void *args_in){
    F* arg = (F*)args_in;
    arg->run();
    pthread_barrier_wait(&barrier); 
    return NULL;
  }
  //init a thread barrier
  void init_threads(){
    pthread_barrier_init (&barrier, NULL, NUM_THREADS+1);
  }
  //join threads on the thread barrier
  void join_threads(){
    pthread_barrier_wait(&barrier);
  }

  ////////////////////////////////////////////////////
  //Code to spin up threads and take work from a queue below
  //Inspired (read copied) from Delite
  //Any Parallel op should be able to be contructed on top of this 
  //thread pool
  ////////////////////////////////////////////////////

  pthread_t* threadPool;
  pthread_mutex_t* locks;
  pthread_cond_t* readyConds;
  pthread_cond_t* doneConds;
  std::atomic<size_t> init_barrier; // barrier synchronization object

  void** workPool;
  void** argPool;

  void initializeThread(size_t threadId) {
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
    void *(*work) (void *);
    void *arg;

    initializeThread(id);
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
      //std::cout << "CREATING: " << i << std::endl; 
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
    delete[] threadPool;
    delete[] locks;
    delete[] readyConds;
    delete[] doneConds;
    delete[] workPool;
    delete[] argPool;
  }
}
#endif