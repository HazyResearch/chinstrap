#ifndef _BLOCK_BITSET_H_
#define _BLOCK_BITSET_H_
/*
THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH THE BITSET LAYOUT.
*/

#include "utils/utils.hpp"

#define BITS_PER_WORD 64
#define ADDRESS_BITS_PER_WORD 6
#define BYTES_PER_WORD 8
#define BLOCK_SIZE 256
#define BITSET_THRESHOLD 0.11
#define WORDS_PER_BLOCK 4

class block_bitset{
  public:
    static size_t word_index(const uint32_t bit_index);
    static bool is_set(const uint32_t index, const uint64_t *in_array, const uint64_t start_index);
    static void set(const uint32_t index, uint64_t *in_array, const uint64_t start_index);

    static long find(uint32_t key, const uint8_t *data_in, const size_t number_of_bytes, const type::layout t);
    static type::layout get_type();
    static std::tuple<size_t,type::layout> build(uint8_t *r_in, const uint32_t *data, const size_t length);
    static std::tuple<size_t,size_t,type::layout> get_flattened_data(const uint8_t *set_data, const size_t cardinality);

    template<typename F>
    static void foreach(
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

    template<typename F>
    static size_t par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);

};
//compute word of data
inline size_t block_bitset::word_index(const uint32_t bit_index){
  return bit_index >> ADDRESS_BITS_PER_WORD;
}
//check if a bit is set
inline bool block_bitset::is_set(const uint32_t index, const uint64_t * const in_array, const uint64_t start_index){
  return (in_array)[word_index(index)-start_index] & ((uint64_t) 1 << (index%BITS_PER_WORD));
}
//check if a bit is set
inline void block_bitset::set(const uint32_t index, uint64_t * const in_array, const uint64_t start_index){
  *(in_array + ((index >> ADDRESS_BITS_PER_WORD)-start_index)) |= ((uint64_t)1 << (index & 0x3F));
}
inline type::layout block_bitset::get_type(){
  return type::BLOCK_BITSET;
}

inline void pack_block(uint64_t *R, const uint32_t *A, const size_t s_a){
  memset(R,(uint8_t)0,BLOCK_SIZE/8);
  for(size_t i = 0; i < s_a; i++){
    const uint32_t cur = A[i];
    const size_t block_bit = cur % BLOCK_SIZE;
    const size_t block_word = block_bit / BITS_PER_WORD;
    R[block_word] |= ((uint64_t) 1 << (block_bit%BITS_PER_WORD)); 
  }

}
inline bool in_range(const uint32_t value, const size_t block_id){
  return (value >= (block_id*BLOCK_SIZE)) && 
        (value < ((block_id+1)*BLOCK_SIZE));
}
//Copies data from input array of ints to our set data r_in
inline std::tuple<size_t,type::layout> block_bitset::build(uint8_t *R, const uint32_t *A, const size_t s_a){
  if(s_a > 0){
    size_t i = 0;
    size_t num_blocks = 0;
    const uint8_t * const R_start = R;
    while(i < s_a){
      const size_t block_id = A[i] / BLOCK_SIZE;
      const size_t block_start_index = i;      
      while(i < s_a && in_range(A[i],block_id)) {
        i++;
      }
      *(uint32_t*)(R) = block_id;
      *(uint32_t*)(R+sizeof(uint32_t)) = i;
      pack_block((uint64_t*)(R+2*sizeof(uint32_t)),&A[block_start_index],(i-block_start_index));
      R += WORDS_PER_BLOCK*sizeof(uint64_t)+2*sizeof(uint32_t);
    }
    const size_t num_bytes = R-R_start;
    return std::make_pair(num_bytes,type::BLOCK_BITSET);
  }
  return std::make_pair(0,type::BLOCK_BITSET);
}

//Iterates over set applying a lambda.
template<typename F>
inline void block_bitset::foreach_until(
    F f,
    const uint8_t *A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type; (void) f; (void) number_of_bytes; (void) A;
  std::cout << "ERROR: NOT IMPLEMENTED" << std::endl;
  abort();
}

template<typename F>
inline void decode_block(
  F f,
  uint32_t offset,
  uint64_t* data){

  for(size_t i = 0; i < BLOCK_SIZE; i++){
    const size_t word = i / BITS_PER_WORD;
    const size_t bit = i % BITS_PER_WORD;
    if((data[word] >> bit) % 2) {
      f(offset*BLOCK_SIZE + i);
    }
  }
} 

//Iterates over set applying a lambda.
template<typename F>
inline void block_bitset::foreach(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;

  uint8_t *A_ptr = const_cast<uint8_t*>(A); 
  if(number_of_bytes > 0){
    const size_t A_num_blocks = number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    for(size_t i = 0; i < A_num_blocks; i++){
      decode_block(f,*(uint32_t*)A_ptr,(uint64_t*)(A_ptr+2*sizeof(uint32_t)));
      A_ptr += 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    }
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline void block_bitset::foreach_index(
    F f,
    const uint8_t * const A,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout type) {
  (void) cardinality; (void) type;
  (void) f; (void) number_of_bytes; (void) A;

  if(number_of_bytes > 0){
    std::cout << "NOT IMPLEMENTED" << std::endl; 
    abort();
  }
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t block_bitset::par_foreach(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
  (void) cardinality; (void) t;

  uint8_t *A_ptr = const_cast<uint8_t*>(A); 
  if(number_of_bytes > 0){
    const size_t A_num_blocks = number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    return par::for_range(0, A_num_blocks, 1,
     [&](size_t tid, size_t i) {
        const uint32_t offset = *(uint32_t*)(A_ptr);
        uint64_t * const data = (uint64_t*)(A_ptr+2*sizeof(uint32_t));
        for(size_t i = 0; i < BLOCK_SIZE; i++){
          const size_t word = i / BITS_PER_WORD;
          const size_t bit = i % BITS_PER_WORD;
          if((data[word] >> bit) % 2) {
            f(tid,offset*BLOCK_SIZE + i);
          }
        }
      A_ptr += 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    });
  }
  return 0;
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t block_bitset::par_foreach_index(
      F f,
      const uint8_t* A,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
  (void) cardinality; (void) t; (void) f; (void) number_of_bytes; (void) A;
  std::cout << "ERROR: NOT IMPLEMENTED2" << std::endl;
  abort();
}

inline long block_bitset::find(uint32_t key, 
  const uint8_t *data_in, 
  const size_t number_of_bytes,
  const type::layout t){
  (void) data_in; (void) number_of_bytes; (void) t; (void) key;
  return -1;
}

#endif