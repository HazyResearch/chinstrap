#ifndef _BITSET_INTERSECTION_H_
#define _BITSET_INTERSECTION_H_

namespace ops{
  template<typename F>
  inline void find_matching_offsets(const uint8_t *A, 
    const size_t lenA, 
    const uint8_t *B, 
    const size_t lenB, 
    F f){


      if (lenA == 0 || lenB == 0)
          return;

      size_t count = 0;
      const size_t increment_value = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
      const uint8_t *endA = A + lenA*increment_value;
      const uint8_t *endB = B + lenB*increment_value;

      while (1) {
          while (*((uint32_t*)A) < *((uint32_t*)B)) {
  SKIP_FIRST_COMPARE:
              A += increment_value;
              if (A == endA)
                  return;
          }
          while (*((uint32_t*)A) > *((uint32_t*)B)) {
              B += increment_value;
              if (B == endB)
                  return;
          }
          if (*((uint32_t*)A) == *((uint32_t*)B)) {
              f(count++,*(uint32_t*)A);
              A += increment_value;
              B += increment_value;
              if (A == endA || B == endB)
                  return;
          } else {
              goto SKIP_FIRST_COMPARE;
          }
      }

      return; // NOTREACHED
  }
  inline size_t intersect_range_block(
    uint64_t * const result_data, 
    uint32_t * const index_data, 
    const uint64_t * const A, 
    const uint64_t * const B,
    const size_t b_size){
    
    size_t i = 0;
    size_t count = 0;

    #if VECTORIZE == 1
    while((i+255) < b_size){
      const size_t vector_index = (i/64);
      const __m256 m1 = _mm256_loadu_ps((float*)(A + vector_index));
      const __m256 m2 = _mm256_loadu_ps((float*)(B + vector_index));
      const __m256 r = _mm256_and_ps(m1, m2);
      
      _mm256_storeu_ps((float*)(result_data+vector_index), r);
      
      index_data[vector_index] = count;
      count += _mm_popcnt_u64(result_data[vector_index]);
      index_data[vector_index+1] = count;
      count += _mm_popcnt_u64(result_data[vector_index+1]);
      index_data[vector_index+2] = count;
      count += _mm_popcnt_u64(result_data[vector_index+2]);
      index_data[vector_index+3] = count;
      count += _mm_popcnt_u64(result_data[vector_index+3]);
      
      i += 256;
    }
    #endif

    //64 bits per word
    for(; i < b_size; i+=64){
      const size_t vector_index = (i/64);
      const uint64_t r = A[vector_index] & B[vector_index]; 
      result_data[vector_index] = r;
      index_data[vector_index] = count;
      count += _mm_popcnt_u64(r);
    }

    return count;
  }

  inline size_t intersect_block(
    uint64_t * const result_data, 
    const uint64_t * const A, 
    const uint64_t * const B,
    const size_t b_size){
    
    size_t i = 0;
    size_t count = 0;

    #if VECTORIZE == 1
    while((i+255) < b_size){
      const size_t vector_index = (i/64);
      const __m256 m1 = _mm256_loadu_ps((float*)(A + vector_index));
      const __m256 m2 = _mm256_loadu_ps((float*)(B + vector_index));
      const __m256 r = _mm256_and_ps(m1, m2);
      
      _mm256_storeu_ps((float*)(result_data+vector_index), r);
      
      count += _mm_popcnt_u64(result_data[vector_index]);
      count += _mm_popcnt_u64(result_data[vector_index+1]);
      count += _mm_popcnt_u64(result_data[vector_index+2]);
      count += _mm_popcnt_u64(result_data[vector_index+3]);
      
      i += 256;
    }
    #endif

    //64 bits per word
    for(; i < b_size; i+=64){
      const size_t vector_index = (i/64);
      const uint64_t r = A[vector_index] & B[vector_index]; 
      result_data[vector_index] = r;
      count += _mm_popcnt_u64(r);
    }

    return count;
  }

  inline Set<range_bitset>* set_intersect(Set<range_bitset> *C_in, const Set<range_bitset> *A_in, const Set<range_bitset> *B_in){
    long count = 0l;
    C_in->number_of_bytes = 0;

    if(A_in->number_of_bytes > 0 && B_in->number_of_bytes > 0){
      const uint64_t *a_index = (uint64_t*) A_in->data;
      const uint64_t *b_index = (uint64_t*) B_in->data;

      uint64_t * const C = (uint64_t*)(C_in->data+sizeof(uint64_t));
      const uint64_t * const A = (uint64_t*)(A_in->data+sizeof(uint64_t));
      const uint64_t * const B = (uint64_t*)(B_in->data+sizeof(uint64_t));
      const size_t s_a = ((A_in->number_of_bytes-sizeof(uint64_t))/(sizeof(uint64_t)+sizeof(uint32_t)));
      const size_t s_b = ((B_in->number_of_bytes-sizeof(uint64_t))/(sizeof(uint64_t)+sizeof(uint32_t)));

      const bool a_big = a_index[0] > b_index[0];
      const uint64_t start_index = (a_big) ? a_index[0] : b_index[0];
      const uint64_t a_start_index = (a_big) ? 0:(b_index[0]-a_index[0]);
      const uint64_t b_start_index = (a_big) ? (a_index[0]-b_index[0]):0;

      const uint64_t end_index = ((a_index[0]+s_a) > (b_index[0]+s_b)) ? (b_index[0]+s_b):(a_index[0]+s_a);
      const uint64_t total_size = (start_index > end_index) ? 0:(end_index-start_index);

      //16 uint16_ts
      //8 ints
      //4 longs

      uint32_t * const index_write = (uint32_t*)(total_size+C);
      uint64_t * const c_index = (uint64_t*) C_in->data;
      c_index[0] = start_index;
    
      count = intersect_range_block(C,index_write,A+a_start_index,B+b_start_index,total_size*64);

      C_in->number_of_bytes = total_size*(sizeof(uint64_t)+sizeof(uint32_t))+sizeof(uint64_t);
    }

    const double density = 0.0;
    C_in->cardinality = count;
    C_in->density = density;
    C_in->type= type::RANGE_BITSET;

    return C_in;
  }
  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in,const Set<block_bitset> *A_in,const Set<block_bitset> *B_in){
    if(A_in->number_of_bytes == 0 || B_in->number_of_bytes == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      C_in->density = 0.0;
      C_in->type= type::BLOCK_BITSET;
      return C_in;
    }

    const size_t A_num_blocks = A_in->number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
    const size_t B_num_blocks = B_in->number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));

    size_t count = 0;
    size_t num_bytes = 0;
    const size_t bytes_per_block = (BLOCK_SIZE/8);

    uint8_t *C = C_in->data;
    const uint8_t * const A_data = A_in->data+2*sizeof(uint32_t);
    const uint8_t * const B_data = B_in->data+2*sizeof(uint32_t);
    const uint32_t offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    find_matching_offsets(A_in->data,A_num_blocks,B_in->data,A_num_blocks, [&](uint32_t index, uint32_t data){
      *((uint32_t*)C) = data/BLOCK_SIZE;
      *((uint32_t*)(C+sizeof(uint32_t))) = count; 
      const size_t old_count = count;
      count += intersect_block((uint64_t*)(C+2*sizeof(uint32_t)),(uint64_t*)(A_data+index*offset),(uint64_t*)(B_data+index*offset),BLOCK_SIZE);
      if(old_count != count){
        C += 2*sizeof(uint32_t)+bytes_per_block;
        num_bytes += 2*sizeof(uint32_t)+bytes_per_block;
      } 
    });   

    C_in->cardinality = count;
    C_in->number_of_bytes = num_bytes;
    C_in->density = 0.0;
    C_in->type= type::BLOCK_BITSET;

    return C_in;
  }
}
#endif
