#ifndef _BLOCK_H_
#define _BLOCK_H_
/*
THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH THE BITSET LAYOUT.
*/

#include "utils/utils.hpp"

#include "block_bitset.hpp"
#include "uinteger.hpp"

class block{
  public:
    static size_t word_index(const uint32_t bit_index);
    static bool is_set(const uint32_t index, const uint64_t *in_array, const uint64_t start_index);
    static void set(const uint32_t index, uint64_t *in_array, const uint64_t start_index);

    static long find(uint32_t key, const uint8_t *data_in, const size_t number_of_bytes,const type::layout t);

    static type::layout get_type();
    static std::tuple<size_t,type::layout> build(uint8_t *r_in, const uint32_t *data, const size_t length);

    template<typename F>
    static void foreach(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t);

    template<typename F>
    static void foreach_until(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t);

    template<typename F>
    static void foreach_index(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const type::layout t);


    template<typename F>
    static size_t par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);

    template<typename F>
    static size_t par_foreach_index(
      F f,
      const uint8_t *data_in,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);
};
inline type::layout block::get_type(){
  return type::BLOCK;
}
//Copies data from input array of ints to our set data r_in
inline std::tuple<size_t,type::layout> block::build(uint8_t *R, const uint32_t *A, const size_t s_a){
  if(s_a > 0){
    size_t i = 0;
    uint32_t *uint_array = new uint32_t[s_a];
    size_t uint_i = 0;
    uint32_t *bitset_array = new uint32_t[s_a];
    size_t bs_i = 0;

    while(i < s_a){
      const size_t block_id = A[i] / BLOCK_SIZE;
      const size_t block_start_index = i;
      while(i < s_a && in_range(A[i],block_id)) {
        i++;
      }

      double density = ((i-block_start_index) < 2) ? 0.0:(double)(i-block_start_index)/BLOCK_SIZE;

      if(density > BITSET_THRESHOLD){
        for(size_t j = block_start_index; j < i; j++){
          bitset_array[bs_i++] = A[j];
        }
      } else{
        for(size_t j = block_start_index; j < i; j++){
          uint_array[uint_i++] = A[j];
        }
      }
    }
    size_t total_bytes_used = 0;
    auto tup = uinteger::build(R+sizeof(size_t),uint_array,uint_i);
    const size_t num_uint_bytes = std::get<0>(tup);
    ((size_t*)R)[0] = num_uint_bytes;
    total_bytes_used += (sizeof(size_t)+num_uint_bytes);
    R += total_bytes_used;

    auto tup2 = block_bitset::build(R,bitset_array,bs_i);
    total_bytes_used += std::get<0>(tup2);
    std::cout << "Num uints: " << num_uint_bytes / sizeof(uint) << std::endl;
    return std::make_pair(total_bytes_used,type::BLOCK);
  }
  return std::make_pair(0,type::BLOCK);
}


//Iterates over set applying a lambda.
template<typename F>
inline void block::foreach_until(
    F f,
    const uint8_t *A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0){
    abort();
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline void block::foreach(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0){
    const size_t num_uint_bytes = ((size_t*)A)[0];
    const uint8_t * const uinteger_data = A+sizeof(size_t);
    const uint8_t * const new_bs_data = A+sizeof(size_t)+num_uint_bytes;
    const size_t num_bs_bytes = number_of_bytes-(sizeof(size_t)+num_uint_bytes);
    const size_t uint_card = num_uint_bytes/sizeof(uint32_t);

    uinteger::foreach(f,uinteger_data,uint_card,num_uint_bytes,type::UINTEGER);
    block_bitset::foreach(f,new_bs_data,cardinality-uint_card,num_bs_bytes,type::BLOCK_BITSET);
  }
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t block::par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
   (void) number_of_bytes; (void) t; (void) cardinality;

  if(number_of_bytes > 0){
    const size_t num_data_words = ((number_of_bytes-sizeof(uint64_t))/sizeof(uint64_t));
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));
    return par::for_range(0, num_data_words, 512,
           [&f, &A64, cardinality,offset](size_t tid, size_t i) {
              const uint64_t cur_word = A64[i];
              if(cur_word != 0) {
                for(size_t j = 0; j < BITS_PER_WORD; j++){
                  const uint32_t curr_nb = BITS_PER_WORD*(i+offset) + j;
                  if((cur_word >> j) % 2) {
                    f(tid, curr_nb);
                  }
                }
              }
           });
  }

  return 1;
}

template<typename F>
inline void block::foreach_index(
  F f,
  const uint8_t *data_in,
  const size_t cardinality,
  const size_t number_of_bytes,
  const type::layout t){
  (void) cardinality; (void) data_in; (void) number_of_bytes; (void) t;

  abort();
}

template<typename F>
inline size_t block::par_foreach_index(
  F f,
  const uint8_t *data_in,
  const size_t cardinality,
  const size_t number_of_bytes,
  const type::layout t){
  abort();
}

inline long block::find(uint32_t key, 
  const uint8_t *data_in, 
  const size_t number_of_bytes,
  const type::layout t){
  (void) data_in; (void) number_of_bytes; (void) t; (void) key;
  return -1;
}

#endif