#ifndef _HETERO_INTERSECTION_H_
#define _HETERO_INTERSECTION_H_

namespace ops{
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

    const size_t B_num_blocks = B_in->number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));

    size_t count = 0;
    uint32_t *C = (uint32_t*)C_in->data;

    const uint32_t * const A_data = (uint32_t*)A_in->data;
    const uint8_t * const B_data = B_in->data+2*sizeof(uint32_t);
    const uint32_t offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);
    
    find_matching_offsets(A_in->data,A_in->cardinality,sizeof(uint32_t),[&](uint32_t a){return a >> ADDRESS_BITS_PER_BLOCK;},
        B_in->data,B_num_blocks,offset,[&](uint32_t b){return b;}, 
        
        //the uinteger value is returned in data->not the best interface 
        [&](uint32_t a_index, uint32_t b_index, uint32_t data){    
          const uint32_t start_a_index = a_index;
          count += probe_block(&C[count],data,(uint64_t*)(B_data+offset*b_index));
          ++a_index;
          while( ( &A_data[a_index] < (A_data+A_in->cardinality) ) &&
            (A_data[a_index] >> ADDRESS_BITS_PER_BLOCK) == (data >> ADDRESS_BITS_PER_BLOCK)){
            count += probe_block(&C[count],A_data[a_index],(uint64_t*)(B_data+offset*b_index));
            ++a_index;
          }
          return std::make_pair(a_index-start_a_index,1);
        }
    );   

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
