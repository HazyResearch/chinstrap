#ifndef _HETERO_INTERSECTION_H_
#define _HETERO_INTERSECTION_H_

namespace ops{
  template<typename F>
  inline size_t probe_block(
    uint32_t *result,
    uint32_t value,
    uint64_t *block,
    F f){
    const uint32_t probe_value = value % BLOCK_SIZE;
    const size_t word_to_check = probe_value / BITS_PER_WORD;
    const size_t bit_to_check = probe_value % BITS_PER_WORD;
    if((block[word_to_check] >> bit_to_check) % 2){
      result[0] = value;
      f(value);
      return 1;
    } 
    return 0;
  }

  template<class N, typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in,const Set<uinteger> *A_in,const Set<range_bitset> *B_in, F f){
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
        C[count] = cur;
        f(cur);
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

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<range_bitset> *A_in, const Set<uinteger> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return set_intersect<unpack_null>(C_in,B_in,A_in,f);
  }

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<range_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return set_intersect<unpack_null>(C_in,A_in,B_in,f);
  }

  template <typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<range_bitset> *A_in, const Set<uinteger> *B_in, F f){
    return set_intersect<unpack_null>(C_in,B_in,A_in,f);
  }

  template <typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<range_bitset> *B_in, F f){
    return set_intersect<unpack_null>(C_in,A_in,B_in,f);
  }

  template<class N, typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in,const Set<uinteger> *A_in,const Set<block_bitset> *B_in, F f){
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
    
    auto check_f = [&](uint32_t d){(void) d; return;};
    auto finish_f = [&](const uint8_t *start, const uint8_t *end, size_t increment){
      (void) start, (void) end; (void) increment;
      return;};
    find_matching_offsets(A_in->data,A_in->cardinality,sizeof(uint32_t),[&](uint32_t a){return a >> ADDRESS_BITS_PER_BLOCK;},check_f,finish_f,
        B_in->data,B_num_blocks,offset,[&](uint32_t b){return b;},check_f,finish_f,
        
        //the uinteger value is returned in data->not the best interface 
        [&](uint32_t a_index, uint32_t b_index, uint32_t data){    
          const uint32_t start_a_index = a_index;
          count += probe_block(&C[count],data,(uint64_t*)(B_data+offset*b_index),f);
          ++a_index;
          while( ( &A_data[a_index] < (A_data+A_in->cardinality) ) &&
            (A_data[a_index] >> ADDRESS_BITS_PER_BLOCK) == (data >> ADDRESS_BITS_PER_BLOCK)){
            count += probe_block(&C[count],A_data[a_index],(uint64_t*)(B_data+offset*b_index),f);
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

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<block_bitset> *A_in, const Set<uinteger> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return set_intersect<unpack_null>(C_in,B_in,A_in,f);
  }

  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<block_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return set_intersect<unpack_null>(C_in,A_in,B_in,f);
  }

  template <typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<block_bitset> *A_in, const Set<uinteger> *B_in, F f){
    return set_intersect<unpack_null>(C_in,B_in,A_in,f);
  }

  template <typename F>
  inline Set<uinteger>* set_intersect(Set<uinteger> *C_in, const Set<uinteger> *A_in, const Set<block_bitset> *B_in, F f){
    return set_intersect<unpack_null>(C_in,A_in,B_in,f);
  }

  template<class N, typename F>
  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<block_bitset> *A_in, const Set<range_bitset> *B_in, F f){
    size_t count = 0;
    size_t num_bytes = 0;

    if(A_in->number_of_bytes > 0 && B_in->number_of_bytes > 0){
      const size_t A_num_blocks = A_in->number_of_bytes/(2*sizeof(uint32_t)+(BLOCK_SIZE/8));
      uint8_t *C = C_in->data;
      const uint32_t offset = 2*sizeof(uint32_t)+WORDS_PER_BLOCK*sizeof(uint64_t);

      const uint64_t *b_index = (uint64_t*) B_in->data;
      const uint64_t * const B = (uint64_t*)(B_in->data+sizeof(uint64_t));
      const size_t s_b = ((B_in->number_of_bytes-sizeof(uint64_t))/(sizeof(uint64_t)+sizeof(uint32_t)));
      const uint64_t b_end = (b_index[0]+s_b);
      const uint64_t b_start = b_index[0];

      for(size_t i = 0; i < A_num_blocks; i++){
        uint32_t index = WORDS_PER_BLOCK * (*((uint32_t*)(A_in->data+i*offset)));
        if( (index+WORDS_PER_BLOCK-1) >= b_start && index < b_end){
          size_t j = 0;
          uint64_t *A_data = (uint64_t*)(A_in->data+i*offset+2*sizeof(uint32_t));
          *((uint32_t*)C) = index/WORDS_PER_BLOCK;
          *((uint32_t*)(C+sizeof(uint32_t))) = count;
          const size_t old_count = count;
          while(index < b_start){
            *((uint64_t*)(C+2*sizeof(uint32_t)+j*sizeof(uint64_t))) = 0;
            index++;
            j++;
          }
          while(j < WORDS_PER_BLOCK && index < b_end){
            const uint64_t result = A_data[j] & B[index-b_start];
            *((uint64_t*)(C+2*sizeof(uint32_t)+j*sizeof(uint64_t))) = result;
            N::unpack(result,index*BITS_PER_WORD,f);
            count += _mm_popcnt_u64(result);
            j++;
            index++;
          }
          while(j < WORDS_PER_BLOCK){
            *((uint64_t*)(C+2*sizeof(uint32_t)+j*sizeof(uint64_t))) = 0;
            j++;
          }
          if(old_count != count){
            num_bytes += offset;
            C += offset;
          }
        }
        if(index >= b_end)
          break;
      }
    }

    const double density = 0.0;
    C_in->cardinality = count;
    C_in->number_of_bytes = num_bytes;
    C_in->density = density;
    C_in->type= type::BLOCK_BITSET;

    return C_in;
  }

  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<range_bitset> *A_in, const Set<block_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return set_intersect<unpack_null_bs>(C_in,B_in,A_in,f);
  }

  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<block_bitset> *A_in, const Set<range_bitset> *B_in){
    auto f = [&](uint32_t data){(void) data; return;};
    return set_intersect<unpack_null_bs>(C_in,A_in,B_in,f);
  }

  template <typename F>
  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<range_bitset> *A_in, const Set<block_bitset> *B_in, F f){
    return set_intersect<unpack_bitset>(C_in,B_in,A_in,f);
  }

  template <typename F>
  inline Set<block_bitset>* set_intersect(Set<block_bitset> *C_in, const Set<block_bitset> *A_in, const Set<range_bitset> *B_in, F f){
    return set_intersect<unpack_bitset>(C_in,A_in,B_in,f);
  }


}
#endif
