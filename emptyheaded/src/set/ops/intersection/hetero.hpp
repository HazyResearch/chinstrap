#ifndef _HETERO_INTERSECTION_H_
#define _HETERO_INTERSECTION_H_

#define ADDRESS_BITS_PER_BLOCK 6

namespace ops{
  inline size_t hetero_intersect_offsets(
    uint32_t *A_positions, 
    uint32_t *B_position_data, 
    uint32_t *A, 
    size_t s_a,
    uint32_t *B, 
    size_t s_b,
    uint64_t *B_data){

    const size_t bytes_per_block = (BLOCK_SIZE/8);
    const size_t words_per_block = bytes_per_block/sizeof(uint64_t);

    size_t count = 0;
    size_t i_a = 0;
    size_t i_b = 0;
    bool notFinished = i_a < s_a  && i_b < s_b;
    while(notFinished){
      while(notFinished && B[i_b] < (A[i_a] >> ADDRESS_BITS_PER_BLOCK)){
        ++i_b;
        notFinished = i_b < s_b;
      }
      if(notFinished && (A[i_a] >> ADDRESS_BITS_PER_BLOCK) == B[i_b]){
        A_positions[count] = i_a;
        B_position_data[count] = i_b;
        _mm_prefetch(&B_data[i_b*words_per_block],_MM_HINT_T1);
        ++count;
      }
      ++i_a;
      notFinished = notFinished && i_a < s_a;
    }
    return count;
  }
  inline size_t probe_block(
    uint32_t *result,
    uint32_t value,
    uint64_t *block){
    const uint32_t probe_value = value % BLOCK_SIZE;
    const size_t word_to_check = probe_value / BITS_PER_WORD;
    const size_t bit_to_check = probe_value % BITS_PER_WORD;
    if((block[word_to_check] >> bit_to_check) % 2){
      #if WRITE_VECTOR == 1
      result[0] = value;
      #else
      (void) result;
      #endif
      return 1;
    } 
    return 0;
  }

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in,const Set<uinteger> *A_in,const Set<range_bitset> *B_in){
    uint32_t * const C = (uint32_t*)C_in->data;
    const uint32_t * const A = (uint32_t*)A_in->data;
    const size_t s_a = A_in->cardinality;
    const size_t s_b = (B_in->number_of_bytes > 0) ? (B_in->number_of_bytes-sizeof(uint64_t))/(sizeof(uint64_t)+sizeof(uint32_t)):0;
    const uint64_t start_index = (B_in->number_of_bytes > 0) ? ((uint64_t*)B_in->data)[0]:0;

    const uint64_t * const B = (uint64_t*)(B_in->data+sizeof(uint64_t));

    #if WRITE_VECTOR == 0
    (void) C;
    #endif

    size_t count = 0;
    for(size_t i = 0; i < s_a; i++){
      const uint32_t cur = A[i];
      const size_t cur_index = range_bitset::word_index(cur);
      if((cur_index < (s_b+start_index)) && (cur_index >= start_index) && range_bitset::is_set(cur,B,start_index)){
        #if WRITE_VECTOR == 1
        C[count] = cur;
        #endif
        count++;
      } else if(cur_index >= (s_b+start_index)){
        break;
      }
    }
    // XXX: Correct density computation
    const double density = 0.0;//((count > 1) ? ((double)count/(C[count - 1]-C[0])) : 0.0);

    C_in->cardinality = count;
    C_in->number_of_bytes = count*sizeof(uint32_t);
    C_in->density = density;
    C_in->type= type::UINTEGER;

    return C_in;
  }
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in,const Set<range_bitset> *A_in,const Set<uinteger> *B_in){
    return set_intersect(C_in,B_in,A_in);
  }

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in,const Set<uinteger> *A_in,const Set<block_bitset> *B_in){
    if(A_in->number_of_bytes == 0 || B_in->number_of_bytes == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      C_in->density = 0.0;
      C_in->type= type::UINTEGER;
      return C_in;
    }

    size_t B_num_blocks = B_in->number_of_bytes/(sizeof(uint32_t)+(BLOCK_SIZE/8));
    uint64_t *B_data = (uint64_t*)(B_in->data+(B_num_blocks*sizeof(uint32_t)));
    uint32_t *B_offset_pointer = (uint32_t*)B_in->data;

    uint32_t *A_data = (uint32_t*)A_in->data;

    size_t A_scratch_space = A_in->cardinality*sizeof(uint32_t);
    //size_t scratch_space = A_scratch_space + B_scratch_space;

    //need to move alloc outsize
    uint32_t *A_positions = (uint32_t*)common::scratch_space[0];
    uint32_t *B_offset_positions = (uint32_t*)(common::scratch_space[0]+A_scratch_space);
    //C_in->data += scratch_space;

    size_t offset_count = hetero_intersect_offsets(
      A_positions,B_offset_positions,A_data,
      A_in->cardinality,B_offset_pointer,B_num_blocks,B_data);

    size_t count = 0;
    const size_t bytes_per_block = (BLOCK_SIZE/8);
    const size_t words_per_block = bytes_per_block/sizeof(uint64_t);
    uint32_t *result = (uint32_t*)C_in->data;
    for(size_t i = 0; i < offset_count; i++){
      const size_t B_offset = B_offset_positions[i] * words_per_block;
      count += probe_block(result+count,A_data[A_positions[i]],B_data+B_offset);
    }
  
    C_in->cardinality = count;
    C_in->number_of_bytes = count*sizeof(uint32_t);
    C_in->density = 0.0;
    C_in->type= type::UINTEGER;

    return C_in;
  }
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in,const Set<block_bitset> *A_in,const Set<uinteger> *B_in){
    return set_intersect(C_in,B_in,A_in);
  }
}
#endif
