#ifndef COMMON_H
#define COMMON_H

#include <x86intrin.h>
#include <unordered_map>
#include <ctime>
#include <sys/time.h>
#include <sys/resource.h>
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <iterator>
#include <algorithm>  // for std::find
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>    /* For O_RDWR */
#include <unistd.h>   /* For open(), creat() */
#include <math.h>
#include <unistd.h>
#include <tuple>
#include <cstdarg>
#include <set>
#include "assert.h"

//static size_t ADDRESS_BITS_PER_BLOCK = 7;
//static size_t BLOCK_SIZE = 128;
//static double BITSET_THRESHOLD = 1.0 / 16.0;

// Experts only! Proceed with caution!
#define ENABLE_PCM
//#define ENABLE_PRINT_THREAD_TIMES
//#define ENABLE_ATOMIC_UNION

//TODO: Replace with new command line arguments.

//Needed for parallelization, prevents false sharing of cache lines
#define PADDING 300
#define MAX_THREADS 512

#define VECTORIZE 1
#define WRITE_VECTOR 1
//#define NO_ALGORITHM

// Enables/disables pruning
//#define NEW_BITSET

// Enables/disables hybrid that always chooses U-Int
//#define HYBRID_UINT

//#define STATS

//CONSTANTS THAT SHOULD NOT CHANGE
#define SHORTS_PER_REG 8
#define INTS_PER_REG 4
#define BYTES_PER_REG 16
#define BYTES_PER_CACHELINE 64

static size_t NUM_THREADS = 48;

namespace common{
  static size_t bitset_length = 0;
  static double bitset_req = (1.0/256.0);

  //HACK
  static uint8_t **scratch_space = new uint8_t*[MAX_THREADS];
  static uint8_t **scratch_space1 = new uint8_t*[MAX_THREADS];

  static void alloc_scratch_space(size_t alloc_size){
    for(size_t i = 0; i < NUM_THREADS; i++){
      scratch_space[i] = new uint8_t[alloc_size];
      scratch_space1[i] = new uint8_t[alloc_size];
    }
  }

}

namespace type{
  enum file : uint8_t{
    csv = 0,
    tsv = 1,
    binary = 2
  };

  enum primitive: uint8_t{
    BOOL = 0,
    UINT32 = 1,
    UINT64 = 2,
    STRING = 3
  };

  enum layout: uint8_t {
    RANGE_BITSET = 0,
    UINTEGER = 1,
    HYBRID = 2,
    BLOCK_BITSET = 3,
    BLOCK = 4
  };

}

#endif
