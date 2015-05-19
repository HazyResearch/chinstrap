#ifndef _BITSET_INTERSECTION_H_
#define _BITSET_INTERSECTION_H_

namespace ops{
 
  inline size_t simd_intersect_offsets(
    uint32_t *C, 
    uint32_t *position_data_A, 
    uint32_t *position_data_B, 
    uint32_t *A, 
    size_t s_a,
    uint32_t *B, 
    size_t s_b,
    uint64_t *A_data,
    uint64_t *B_data){    

    const size_t bytes_per_block = (BLOCK_SIZE/8);
    const size_t words_per_block = bytes_per_block/sizeof(uint64_t);

    size_t count = 0;
    size_t i_a = 0, i_b = 0;

    // trim lengths to be a multiple of 4
    #if VECTORIZE == 1
    size_t st_a = (s_a / 4) * 4;
    size_t st_b = (s_b / 4) * 4;
    uint32_t offset[] = {0,1,2,3};
    const __m128i offset_reg = _mm_loadu_si128((__m128i*)&offset);

    while(i_a < st_a && i_b < st_b) {
      //[ load segments of four 32-bit elements
      __m128i v_a = _mm_loadu_si128((__m128i*)&A[i_a]);
      __m128i v_b = _mm_loadu_si128((__m128i*)&B[i_b]);
      //]

      const __m128i a_offset = _mm_set1_epi32(i_a);

      //[ move pointers
      uint32_t a_max = A[i_a+3];
      uint32_t b_max = B[i_b+3];
      //]

      //[ compute mask of common elements
      const uint32_t right_cyclic_shift = _MM_SHUFFLE(0,3,2,1);
      __m128i cmp_mask1 = _mm_cmpeq_epi32(v_a, v_b);    // pairwise comparison
      v_b = _mm_shuffle_epi32(v_b, right_cyclic_shift);       // shuffling
      __m128i cmp_mask2 = _mm_cmpeq_epi32(v_a, v_b);    // again...
      v_b = _mm_shuffle_epi32(v_b, right_cyclic_shift);
      __m128i cmp_mask3 = _mm_cmpeq_epi32(v_a, v_b);    // and again...
      v_b = _mm_shuffle_epi32(v_b, right_cyclic_shift);
      __m128i cmp_mask4 = _mm_cmpeq_epi32(v_a, v_b);    // and again.
      __m128i cmp_mask = _mm_or_si128(
              _mm_or_si128(cmp_mask1, cmp_mask2),
              _mm_or_si128(cmp_mask3, cmp_mask4)
      ); // OR-ing of comparison masks
      // convert the 128-bit mask to the 4-bit mask
      uint32_t mask = _mm_movemask_ps((__m128)cmp_mask);
      //]
      //[ copy out common elements
      //#if WRITE_VECTOR == 1
      __m128i r = _mm_shuffle_epi8(v_a, masks::shuffle_mask32[mask]);
      _mm_storeu_si128((__m128i*)&C[count], r);

      const __m128i p1 = _mm_shuffle_epi8(offset_reg, masks::shuffle_mask32[mask]);
      _mm_storeu_si128((__m128i*)&position_data_A[count], _mm_add_epi32(p1,a_offset));

      //const __m128i p2 = _mm_shuffle_epi8(offset_reg, shuffle_mask32[mask]);
      //_mm_storeu_si128((__m128i*)&position_data_B[count], _mm_add_epi32(p2,b_offset));

      const size_t tmp_count = _mm_popcnt_u32(mask);;
      size_t b_index = 0;
      for(size_t i = count; i < (tmp_count+count); i ++){
        while(B[i_b+b_index] < C[i]){
          b_index++;
        }
        position_data_B[i] = b_index+i_b;
        _mm_prefetch(&A_data[position_data_A[i]*words_per_block],_MM_HINT_T1);
        _mm_prefetch(&B_data[position_data_B[i]*words_per_block],_MM_HINT_T1);
      }
      //cout << "C[" << count << "]: " << C[count] << endl;

      //#endif

      i_a += (a_max <= b_max) * 4;
      i_b += (a_max >= b_max) * 4;
      count += tmp_count;
       // a number of elements is a weight of the mask
      //]
    }
    #endif

  // intersect the tail using scalar intersection
    bool notFinished = i_a < s_a  && i_b < s_b;
    while(notFinished){
      while(notFinished && B[i_b] < A[i_a]){
        ++i_b;
        notFinished = i_b < s_b;
      }
      if(notFinished && A[i_a] == B[i_b]){
        C[count] = A[i_a];
        position_data_A[count] = i_a;
        position_data_B[count] = i_b;
        _mm_prefetch(&A_data[i_a*words_per_block],_MM_HINT_T1);
        _mm_prefetch(&B_data[i_b*words_per_block],_MM_HINT_T1);
        ++count;
      }
      ++i_a;
      notFinished = notFinished && i_a < s_a;
    }

    #if WRITE_VECTOR == 0
    (void) C;
    #endif

    return count;  
  }

  inline size_t intersect_block(
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
    
      count = intersect_block(C,index_write,A+a_start_index,B+b_start_index,total_size*64);

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

    const size_t A_num_blocks = A_in->number_of_bytes/(sizeof(uint32_t)+(BLOCK_SIZE/8));
    const size_t B_num_blocks = B_in->number_of_bytes/(sizeof(uint32_t)+(BLOCK_SIZE/8));

    const size_t A_scratch_space = A_num_blocks*sizeof(uint64_t)*2;
    uint32_t *A_offset_positions = (uint32_t*)(common::scratch_space[0]); //FIXME
    uint32_t *B_offset_positions = (uint32_t*)(common::scratch_space[0]+A_scratch_space); //FIXME

    uint64_t *A_data = (uint64_t*)(A_in->data+(A_num_blocks*sizeof(uint32_t)));
    uint64_t *B_data = (uint64_t*)(B_in->data+(B_num_blocks*sizeof(uint32_t)));

    uint32_t *A_offset_pointer = (uint32_t*)A_in->data;
    uint32_t *B_offset_pointer = (uint32_t*)B_in->data;

    size_t offset_count = simd_intersect_offsets((uint32_t *)C_in->data,
      A_offset_positions,B_offset_positions,
      A_offset_pointer,A_num_blocks,
      B_offset_pointer,B_num_blocks,
      A_data,B_data);

    uint64_t *result = (uint64_t*)(C_in->data + sizeof(uint32_t)*offset_count);
    const size_t bytes_per_block = (BLOCK_SIZE/8);
    const size_t words_per_block = bytes_per_block/sizeof(uint64_t);
    size_t count = 0;
    for(size_t i = 0; i < offset_count; i++){
      const size_t A_offset = A_offset_positions[i] * words_per_block;
      const size_t B_offset = B_offset_positions[i] * words_per_block;
      count += 0;//FIXME: intersect_block(result+(i*words_per_block),A_data+A_offset,B_data+B_offset);
    }

    C_in->cardinality = count;
    C_in->number_of_bytes = offset_count*(sizeof(uint32_t)+bytes_per_block);
    C_in->density = 0.0;
    C_in->type= type::BLOCK_BITSET;

    return C_in;
  }
}
#endif
