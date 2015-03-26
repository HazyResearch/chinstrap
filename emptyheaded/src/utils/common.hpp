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
#include "parallel.hpp"
#include "assert.h"

//static size_t ADDRESS_BITS_PER_BLOCK = 7;
//static size_t BLOCK_SIZE = 128;
//static double BITSET_THRESHOLD = 1.0 / 16.0;

// Experts only! Proceed wih caution!
//#define ENABLE_PCM
//#define ENABLE_PRINT_THREAD_TIMES
//#define ENABLE_ATOMIC_UNION

//TODO: Replace with new command line arguments.

#define ALLOCATOR 4
#define REALLOC_THRESHOLD 0.9

//Needed for parallelization, prevents false sharing of cache lines
#define PADDING 300
#define MAX_THREADS 512

//#define ATTRIBUTES
#define WRITE_TABLE 0

#define COMPRESSION 0
#define PERFORMANCE 1
#define VECTORIZE 1

// Enables/disables pruning
#define PRUNING
//#define NEW_BITSET

// Enables/disables hybrid that always chooses U-Int
//#define HYBRID_UINT

//#define STATS

//CONSTANTS THAT SHOULD NOT CHANGE
#define SHORTS_PER_REG 8
#define INTS_PER_REG 4
#define BYTES_PER_REG 16

namespace common{
  //static size_t bitset_length = 0;
  //static size_t pshort_requirement = 16;
  //static double bitset_req = (1.0/256.0);

  static double startClock (){
    return 0.0;
  }

  static double stopClock(double t_in){
    double t2=0.0;
    return t2 - t_in;
  }
  static double stopClock(std::string in,double t_in){
    double t2=0.0;
    std::cout << "Time["+in+"]: " << t2-t_in << " s" << std::endl;
    return t2 - t_in;
  }

  static void allocateStack(){
    const rlim_t kStackSize = 64L * 1024L * 1024L;   // min stack size = 64 Mb
    struct rlimit rl;
    int result;

    result = getrlimit(RLIMIT_STACK, &rl);
    if (result == 0){
      if (rl.rlim_cur < kStackSize){
        rl.rlim_cur = kStackSize;
        result = setrlimit(RLIMIT_STACK, &rl);
        if (result != 0){
          fprintf(stderr, "setrlimit returned result = %d\n", result);
        }
      }
    }
  }

  static void _mm256_print_ps(__m256 x) {
    float *data = new float[8];
    _mm256_storeu_ps(&data[0],x);
    for(size_t i =0 ; i < 8; i++){
      std::cout << "Data[" << i << "]: " << data[i] << std::endl;
    }
    delete[] data;
  }
  static void _mm128i_print_shorts(__m128i x) {
    for(size_t i =0 ; i < 8; i++){
      std::cout << "Data[" << i << "]: " << _mm_extract_epi16(x,i) << std::endl;
    }
  }
  static void _mm128i_print(__m128i x) {
    for(size_t i =0 ; i < 4; i++){
      std::cout << "Data[" << i << "]: " << _mm_extract_epi32(x,i) << std::endl;
    }
  }
  static void _mm256i_print(__m256i x) {
    int *data = new int[8];
    _mm256_storeu_si256((__m256i*)&data[0],x);
    for(size_t i =0 ; i < 8; i++){
      std::cout << "Data[" << i << "]: " << data[i] << std::endl;
    }
    delete[] data;
  }
  //Thanks stack overflow.
  static inline float _mm256_reduce_add_ps(__m256 x) {
    /* ( x3+x7, x2+x6, x1+x5, x0+x4 ) */
    const __m128 x128 = _mm_add_ps(_mm256_extractf128_ps(x, 1), _mm256_castps256_ps128(x));
    /* ( -, -, x1+x3+x5+x7, x0+x2+x4+x6 ) */
    const __m128 x64 = _mm_add_ps(x128, _mm_movehl_ps(x128, x128));
    /* ( -, -, -, x0+x1+x2+x3+x4+x5+x6+x7 ) */
    const __m128 x32 = _mm_add_ss(x64, _mm_shuffle_ps(x64, x64, 0x55));
    /* Conversion to float is a no-op on x86-64 */
    return _mm_cvtss_f32(x32);
  }

  static size_t num_bs = 0;
  static size_t num_pshort = 0;
  static size_t num_uint = 0;
  static size_t num_bp = 0;
  static size_t num_v = 0;
  static double bits_per_edge = 0;
  static double bits_per_edge_nometa = 0;

  static size_t num_uint_uint = 0;
  static size_t num_pshort_pshort = 0;
  static size_t num_bs_bs = 0;
  static size_t num_uint_pshort = 0;
  static size_t num_uint_bs = 0;
  static size_t num_pshort_bs = 0;

  enum type: uint8_t {
    BITSET = 0,
    PSHORT = 1,
    UINTEGER = 2,
    BITPACKED = 3,
    VARIANT = 4,
    HYBRID = 5,
    KUNLE = 6,
    BITSET_NEW = 7,
    NEW_TYPE = 8
  };

  enum graph_type {
    DIRECTED,
    UNDIRECTED
  };

  static void dump_stats(){
    std::cout << std::endl;
    std::cout << "Num Bitset: " << num_bs << std::endl;
    std::cout << "Num PShort: " << num_pshort
     << std::endl;
    std::cout << "Num Uint: " << num_uint << std::endl;
    std::cout << "Num BP: " << num_bp << std::endl;
    std::cout << "Num V: " << num_v << std::endl;
    std::cout << "Bits per edge (meta): " << bits_per_edge << std::endl;
    std::cout << "Bits per edge (no meta): " << bits_per_edge_nometa << std::endl;

    std::cout << "Num UINT/UINT: " << num_uint_uint << std::endl;
    std::cout << "Num UINT/PSHORT: " << num_uint_pshort << std::endl;
    std::cout << "Num UINT/BS: " << num_uint_bs << std::endl;
    std::cout << "Num PSHORT/PSHORT: " << num_pshort_pshort << std::endl;
    std::cout << "Num PSHORT/BS: " << num_pshort_bs << std::endl;
    std::cout << "Num BS/BS: " << num_bs_bs << std::endl;
  }
}
#endif
