#ifndef _RANGE_range_bitset_H_
#define _RANGE_range_bitset_H_
/*
THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH THE range_bitset LAYOUT.
*/

#include "utils/utils.hpp"

#define BITS_PER_WORD 64
#define ADDRESS_BITS_PER_WORD 6
#define BYTES_PER_WORD 8

class range_bitset{
  public:
    static size_t word_index(const uint32_t bit_index);
    static bool is_set(const uint32_t index, const uint64_t *in_array, const uint64_t start_index);
    static void set(const uint32_t index, uint64_t *in_array, const uint64_t start_index);

    static type::layout get_type();
    static std::tuple<size_t,type::layout> build(uint8_t *r_in, const uint32_t *data, const size_t length);
    static size_t get_number_of_words(size_t num_bytes);

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
    static size_t par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);
};
inline size_t range_bitset::get_number_of_words(size_t num_bytes){
  return ((num_bytes-sizeof(uint64_t))/(BYTES_PER_WORD+sizeof(uint32_t)));
}
//compute word of data
inline size_t range_bitset::word_index(const uint32_t bit_index){
  return bit_index >> ADDRESS_BITS_PER_WORD;
}
//check if a bit is set
inline bool range_bitset::is_set(const uint32_t index, const uint64_t * const in_array, const uint64_t start_index){
  return (in_array)[word_index(index)-start_index] & ((uint64_t) 1 << (index%BITS_PER_WORD));
}
//check if a bit is set
inline void range_bitset::set(const uint32_t index, uint64_t * const in_array, const uint64_t start_index){
  *(in_array + ((index >> ADDRESS_BITS_PER_WORD)-start_index)) |= ((uint64_t)1 << (index & 0x3F));
}
inline type::layout range_bitset::get_type(){
  return type::RANGE_BITSET;
}
//Copies data from input array of ints to our set data r_in
inline std::tuple<size_t,type::layout> range_bitset::build(uint8_t *R, const uint32_t *A, const size_t s_a){
  if(s_a > 0){
    const uint64_t offset = word_index(A[0]);
    ((uint64_t*)R)[0] = offset; 

    uint64_t* R64_data = (uint64_t*)(R+sizeof(uint64_t));
    size_t word = word_index(A[0]);
    size_t i = 0;

    const size_t num_words = (word_index(A[s_a-1])-offset)+1;
    memset(R64_data,(uint8_t)0,num_words*sizeof(uint64_t));
    uint32_t* R32_index = (uint32_t*)(&R64_data[num_words]);

    while(i < s_a){
      uint32_t cur = A[i];
      word = word_index(cur);
      uint64_t set_value = (uint64_t) 1 << (cur % BITS_PER_WORD);
      bool same_word = true;
      ++i;
      while(i<s_a && same_word){
        if(word_index(A[i])==word){
          cur = A[i];
          set_value |= ((uint64_t) 1 << (cur%BITS_PER_WORD));
          ++i;
        } else same_word = false;
      }
      R64_data[word-offset] = set_value;
      R32_index[word-offset] = (i-1);
    }
    const size_t num_bytes = (num_words)*(BYTES_PER_WORD+sizeof(uint32_t)) + sizeof(uint64_t);
    return std::make_pair(num_bytes,type::RANGE_BITSET);
  }
  return std::make_pair(0,type::RANGE_BITSET);
}
//Iterates over set applying a lambda.
template<typename F>
inline void range_bitset::foreach_until(
    F f,
    const uint8_t *A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));
    for(size_t i = 0; i < num_data_words; i++){
      const uint64_t cur_word = A64[i];
      for(size_t j = 0; j < BITS_PER_WORD; j++){
        if((cur_word >> j) % 2){
          if(f(BITS_PER_WORD*(i+offset) + j))
            break;
        }
      }
    }
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline void range_bitset::foreach(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));

    for(size_t i = 0; i < num_data_words; i++){
      const uint64_t cur_word = *A64;
      if(cur_word != 0) {
        for(size_t j = 0; j < BITS_PER_WORD; j++){
          if((cur_word >> j) % 2) {
            f(BITS_PER_WORD *(i+offset) + j);
          }
        }
      }
      A64++;
    }
  }
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t range_bitset::par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
   (void) number_of_bytes; (void) t; (void) cardinality;

  if(number_of_bytes > 0){
    const size_t num_data_words = get_number_of_words(number_of_bytes);
    const uint64_t offset = ((uint64_t*)A)[0];
    const uint64_t* A64 = (uint64_t*)(A+sizeof(uint64_t));
    return par::for_range(0, num_data_words, 1,
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

#endif