#ifndef _BLOCK_INTERSECTION_H_
#define _BLOCK_INTERSECTION_H_

#include "uinteger.hpp"
#include "bitset.hpp"
#include "hetero.hpp"

namespace ops{
  inline void distinct_merge_three_way(
    const size_t count,
    uint32_t *result, 
    const uint32_t *A, const size_t lenA, 
    const uint32_t *B, const size_t lenB,
    const uint32_t *C, const size_t lenC){

    size_t i_a = 0;
    size_t i_b = 0;
    size_t i_c = 0;
    for(size_t i=0; i < count; i++){
      if(i_a < lenA && i_b < lenB && i_c < lenC){
        if(A[i_a] <= B[i_b] && A[i_a] <= C[i_c]){
          result[i] = A[i_a++];
        } else if(B[i_b] <= A[i_a] && B[i_b] <= C[i_c]){
          result[i] = B[i_b++];
        } else{
          result[i] = C[i_c++];
        }
      } else if(i_b < lenB && i_c < lenC){
        if(B[i_b] <= C[i_c]){
          result[i] = B[i_b++];
        } else{
          result[i] = C[i_c++];
        }
      } else if(i_a < lenA && i_c < lenC){
        if(A[i_a] <= C[i_c]){
          result[i] = A[i_a++];
        } else{
          result[i] = C[i_c++];
        }
      } else if(i_a < lenA && i_b < lenB){
        if(A[i_a] <= B[i_b]){
          result[i] = A[i_a++];
        } else{
          result[i] = B[i_b++];
        }
      } else if(i_a < lenA){
        result[i] = A[i_a++];
      } else if(i_b < lenB){
        result[i] = B[i_b++];
      } else{
        result[i] = C[i_c++];
      }
    }
  }
  inline Set<block>* set_intersect(Set<block> *C_in,const Set<block> *A_in,const Set<block> *B_in){
    if(A_in->number_of_bytes == 0 || B_in->number_of_bytes == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      C_in->density = 0.0;
      C_in->type= type::BLOCK;
      return C_in;
    }

    const size_t A_num_uint_bytes = ((size_t*)A_in->data)[0];
    uint8_t * A_uinteger_data = A_in->data+sizeof(size_t);
    uint8_t * A_new_bs_data = A_in->data+sizeof(size_t)+A_num_uint_bytes;
    const size_t A_num_bs_bytes = A_in->number_of_bytes-(sizeof(size_t)+A_num_uint_bytes);
    const size_t A_uint_card = A_num_uint_bytes/sizeof(uint32_t);

    const size_t B_num_uint_bytes = ((size_t*)B_in->data)[0];
    uint8_t * B_uinteger_data = B_in->data+sizeof(size_t);
    uint8_t * B_new_bs_data = B_in->data+sizeof(size_t)+B_num_uint_bytes;
    const size_t B_num_bs_bytes = B_in->number_of_bytes-(sizeof(size_t)+B_num_uint_bytes);
    const size_t B_uint_card = B_num_uint_bytes/sizeof(uint32_t);

    //do all three uintegers then merge then intersect the bs
    const size_t scratch1_space = A_num_uint_bytes;
    uint8_t *scratch1 = new uint8_t[sizeof(uint64_t)*(A_in->cardinality+B_in->cardinality)]; //FIXME
    const size_t scratch2_space = A_num_uint_bytes;
    uint8_t *scratch2 = scratch1+scratch1_space;    
    uint8_t *scratch3 = scratch2+scratch2_space; 
    //C_in->data += (scratch1_space+scratch2_space+scratch3_space);  

    const Set<uinteger>A_I(A_uinteger_data,A_uint_card,A_num_uint_bytes,type::UINTEGER);
    const Set<uinteger>B_I(B_uinteger_data,B_uint_card,B_num_uint_bytes,type::UINTEGER);

    const Set<block_bitset>A_BS(A_new_bs_data,A_in->cardinality-A_uint_card,A_num_bs_bytes,type::BLOCK_BITSET);
    const Set<block_bitset>B_BS(B_new_bs_data,B_in->cardinality-B_uint_card,B_num_bs_bytes,type::BLOCK_BITSET);

    size_t count = 0;
    
    uint8_t *C_pointer = C_in->data+sizeof(size_t);
    if( (A_I.number_of_bytes != 0 || B_I.number_of_bytes != 0) &&
      (A_BS.number_of_bytes != 0 || B_BS.number_of_bytes != 0)){
      
      Set<uinteger>UU(scratch1);
      Set<uinteger>UBS(scratch2);
      Set<uinteger>BSU(scratch3);

      UU = ops::set_intersect(&UU,&A_I,&B_I);
      count += UU.cardinality;

      UBS = ops::set_intersect(&UBS,&A_I,&B_BS);
      count += UBS.cardinality;

      BSU = ops::set_intersect(&BSU,&B_I,&A_BS);
      count += BSU.cardinality;

      #if WRITE_VECTOR == 1
      distinct_merge_three_way(
        count,
        (uint32_t*)(C_pointer),
        (uint32_t*)UU.data,UU.cardinality, 
        (uint32_t*)UBS.data,UBS.cardinality,
        (uint32_t*)BSU.data,BSU.cardinality);
      #endif
    } else if(A_BS.number_of_bytes == 0 && B_BS.number_of_bytes == 0){
      Set<uinteger>UU(C_pointer);
      UU = ops::set_intersect(&UU,&A_I,&B_I);
      count += UU.cardinality;

      ((size_t*)C_in->data)[0] = (count*sizeof(uint32_t));
      C_in->cardinality = count;
      C_in->number_of_bytes = sizeof(size_t)+(count*sizeof(uint32_t));
      C_in->density = 0.0;
      C_in->type= type::BLOCK;

      return C_in;
    }
    
    const size_t num_uint = count;
    ((size_t*)C_in->data)[0] = (num_uint*sizeof(uint32_t));
    C_pointer += (num_uint*sizeof(uint32_t));

    Set<block_bitset>BSBS(C_pointer);
    BSBS = ops::set_intersect(&BSBS,&A_BS,&B_BS);
    count += BSBS.cardinality;

    C_in->cardinality = count;
    C_in->number_of_bytes = sizeof(size_t)+(num_uint*sizeof(uint32_t))+BSBS.number_of_bytes;
    C_in->density = 0.0;
    C_in->type= type::BLOCK;

    return C_in;
  }
}
#endif
