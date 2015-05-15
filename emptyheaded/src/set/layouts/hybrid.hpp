#ifndef _HYBRID_H_
#define _HYBRID_H_

/*
THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH AN HYBRID SET LAYOUT.
HERE WE DYNAMICALLY DETECT THE UNDERLYING LAYOUT OF THE SET AND
RUN THE CORRESPONDING SET MEHTODS.
*/

#include "uinteger.hpp"
#include "range_bitset.hpp"

class hybrid{
  public:
    static type::layout get_type();
    static type::layout get_type(const uint32_t *data, const size_t length);
    static std::tuple<size_t,type::layout> build(uint8_t *r_in, const uint32_t *data, const size_t length);

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
      const uint8_t *data_in,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);

    template<typename F>
    static size_t par_foreach_index(
      F f,
      const uint8_t *data_in,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t);
};
inline type::layout hybrid::get_type(){
  return type::HYBRID;
}

inline type::layout hybrid::get_type(const uint32_t *data, const size_t length){
  if(length > 1) {
    uint32_t range = data[length - 1] - data[0];
    if(range > 0){
      double density = (double) length / range;
     // double c = compressibility(data, length);
      if(density > ((double)common::bitset_req) && length > common::bitset_length) {
        return type::RANGE_BITSET;
      } //else if( (length/(range/BLOCK_SIZE)  > common::pshort_requirement)) {
        //return common::BITSET_NEW;
      //}
       else {
        return type::UINTEGER;
      }
    }
    return type::UINTEGER;
  } else{
    return type::UINTEGER;
  }
}

//Copies data from input array of ints to our set data r_in
inline std::tuple<size_t,type::layout> hybrid::build(uint8_t *r_in, const uint32_t *data, const size_t length){
  type::layout t = hybrid::get_type(data,length);
  switch(t){
    case type::UINTEGER :
      return std::make_pair(std::get<0>(uinteger::build(r_in,data,length)),t);
    break;
    case type::RANGE_BITSET :
      return std::make_pair(std::get<0>(range_bitset::build(r_in,data,length)),t);
    break;
    default:
      return std::make_pair(0,t);
  }
}
//Iterates over set applying a lambda.
template<typename F>
inline void hybrid::foreach_until(
    F f,
    const uint8_t *data_in,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout t) {
  switch(t){
    case type::UINTEGER:
      uinteger::foreach_until(f,data_in,cardinality,number_of_bytes,type::UINTEGER);
      break;
    case type::RANGE_BITSET:
      range_bitset::foreach_until(f,data_in,cardinality,number_of_bytes,type::RANGE_BITSET);
      break;
    default:
      break;
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline void hybrid::foreach(
    F f,
    const uint8_t *data_in,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout t) {
  switch(t){
    case type::UINTEGER :
      uinteger::foreach(f,data_in,cardinality,number_of_bytes,type::UINTEGER);
      break;
    case type::RANGE_BITSET :
      range_bitset::foreach(f,data_in,cardinality,number_of_bytes,type::RANGE_BITSET);
      break;
    default:
      break;
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline void hybrid::foreach_index(
    F f,
    const uint8_t *data_in,
    const size_t cardinality,
    const size_t number_of_bytes,
    const type::layout t) {
  switch(t){
    case type::UINTEGER :
      uinteger::foreach_index(f,data_in,cardinality,number_of_bytes,type::UINTEGER);
      break;
    case type::RANGE_BITSET :
      range_bitset::foreach_index(f,data_in,cardinality,number_of_bytes,type::RANGE_BITSET);
      break;
    default:
      break;
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline size_t hybrid::par_foreach(
      F f,
      const uint8_t *data_in,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
  switch(t){
    case type::UINTEGER :
      return uinteger::par_foreach(f,data_in,cardinality,number_of_bytes,type::UINTEGER);
      break;
    case type::RANGE_BITSET :
      return range_bitset::par_foreach(f,data_in,cardinality,number_of_bytes,type::RANGE_BITSET);
      break;
    default:
      return 0;
      break;
  }
}

//Iterates over set applying a lambda.
template<typename F>
inline size_t hybrid::par_foreach_index(
      F f,
      const uint8_t *data_in,
      const size_t cardinality,
      const size_t number_of_bytes,
      const type::layout t) {
  switch(t){
    case type::UINTEGER :
      return uinteger::par_foreach_index(f,data_in,cardinality,number_of_bytes,type::UINTEGER);
      break;
    case type::RANGE_BITSET :
      return range_bitset::par_foreach_index(f,data_in,cardinality,number_of_bytes,type::RANGE_BITSET);
      break;
    default:
      return 0;
      break;
  }
}

#endif