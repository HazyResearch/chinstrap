#ifndef _SET_INTERSECTION_H_
#define _SET_INTERSECTION_H_

#include "uinteger.hpp"
#include "bitset.hpp"
#include "hetero.hpp"

namespace ops{
inline Set<hybrid>* set_intersect(Set<hybrid> *C_in,const Set<hybrid> *A_in,const Set<hybrid> *B_in){
    if(A_in->cardinality == 0 || B_in->cardinality == 0){
      C_in->cardinality = 0;
      C_in->number_of_bytes = 0;
      return C_in;
    }

    switch (A_in->type) {
        case type::UINTEGER:
          switch (B_in->type) {
            case type::UINTEGER:
              #ifdef STATS
              debug::num_uint_uint++;
              #endif
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)A_in,(const Set<uinteger>*)B_in);
              break;
            case type::RANGE_BITSET:
              #ifdef STATS
              type::num_uint_bs++;
              #endif
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)A_in,(const Set<range_bitset>*)B_in);
              break;
            default:
              break;
          }
        break;
        case type::RANGE_BITSET:
          switch (B_in->type) {
            case type::UINTEGER:
              #ifdef STATS
              debug::num_uint_bs++;
              #endif
              return (Set<hybrid>*)set_intersect((Set<uinteger>*)C_in,(const Set<uinteger>*)B_in,(const Set<range_bitset>*)A_in);
            break;
            case type::RANGE_BITSET:
              #ifdef STATS
              debug::num_bs_bs++;
              #endif
              return (Set<hybrid>*)set_intersect((Set<range_bitset>*)C_in,(const Set<range_bitset>*)A_in,(const Set<range_bitset>*)B_in);
            break;
            default:
            break;
          }
        break;
        default:
        break;
    }

    std::cout << "ERROR" << std::endl;
    return C_in;
  }
}
#endif
