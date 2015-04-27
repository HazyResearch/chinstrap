#ifndef _BITSET_H_
#define _BITSET_H_
/*
THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH THE BITSET LAYOUT.
*/

#include "utils/utils.hpp"

#define BITS_PER_WORD 64
#define ADDRESS_BITS_PER_WORD 6
#define BYTES_PER_WORD 8

class bitset{
  public:
    static size_t word_index(const uint32_t bit_index);
    static bool is_set(const uint32_t index, const uint64_t *in_array, const uint64_t start_index);
    static void set(const uint32_t index, uint64_t *in_array, const uint64_t start_index);

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
    static size_t par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);
};
//compute word of data
inline size_t bitset::word_index(const uint32_t bit_index){
  return bit_index >> ADDRESS_BITS_PER_WORD;
}
//check if a bit is set
inline bool bitset::is_set(const uint32_t index, const uint64_t * const in_array, const uint64_t start_index){
  return (in_array)[word_index(index)-start_index] & ((uint64_t) 1 << (index%BITS_PER_WORD));
}
//check if a bit is set
inline void bitset::set(const uint32_t index, uint64_t * const in_array, const uint64_t start_index){
  *(in_array + ((index >> ADDRESS_BITS_PER_WORD)-start_index)) |= ((uint64_t)1 << (index & 0x3F));
}
inline type::layout bitset::get_type(){
  return type::BITSET;
}
//Copies data from input array of ints to our set data r_in
inline std::tuple<size_t,type::layout> bitset::build(uint8_t *R, const uint32_t *A, const size_t s_a){
  if(s_a > 0){
    const uint64_t offset = word_index(A[0]);
    ((uint64_t*)R)[0] = offset; 

    uint64_t* R64 = (uint64_t*)(R+sizeof(uint64_t));
    size_t word = word_index(A[0]);
    size_t i = 0;
    size_t num_words_to_clear = word_index(A[s_a-1])-offset;
    memset(R64,(uint8_t)0,num_words_to_clear*sizeof(uint64_t));

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
      R64[word-offset] = set_value;
    }
    const size_t num_bytes = ((word-offset+1) * BYTES_PER_WORD) + sizeof(uint64_t);
    return std::make_pair(num_bytes,type::BITSET);
  }
  return std::make_pair(0,type::BITSET);
}
//Iterates over set applying a lambda.
template<typename F>
inline void bitset::foreach_until(
    F f,
    const uint8_t *A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0){
    const size_t num_data_words = ((number_of_bytes-sizeof(uint64_t))/sizeof(uint64_t));
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
inline void bitset::foreach(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  if(number_of_bytes > 0){
    const size_t num_data_words = ((number_of_bytes-sizeof(uint64_t))/sizeof(uint64_t));
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
inline size_t bitset::par_foreach(
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

#endif