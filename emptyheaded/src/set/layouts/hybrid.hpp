#ifndef _HYBRID_H_
#define _HYBRID_H_

/*
THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH AN HYBRID SET LAYOUT.
HERE WE DYNAMICALLY DETECT THE UNDERLYING LAYOUT OF THE SET AND
RUN THE CORRESPONDING SET MEHTODS.
*/

#include "uinteger.hpp"
#include "bitset.hpp"

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
        return type::BITSET;
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
    case type::BITSET :
      return std::make_pair(std::get<0>(bitset::build(r_in,data,length)),t);
    break;
    /*
    case common::BITSET_NEW :
      return bitset_new::build(r_in,data,length);
    break;
    case common::PSHORT :
      return pshort::build(r_in,data,length);
    break;
    case common::VARIANT :
      return variant::build(r_in,data,length);
    break;
    case common::BITPACKED :
      return bitpacked::build(r_in,data,length);
    break;
    */
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
      uinteger::foreach(f,data_in,cardinality,number_of_bytes,type::UINTEGER);
      break;
    case type::BITSET:
      bitset::foreach(f,data_in,cardinality,number_of_bytes,type::BITSET);
      break;
      /*
    case common::BITSET_NEW:
      bitset_new::foreach(f,data_in,cardinality,number_of_bytes,common::BITSET_NEW);
      break;
    case common::PSHORT:
      pshort::foreach(f,data_in,cardinality,number_of_bytes,common::PSHORT);
      break;
    case common::VARIANT:
      variant::foreach(f,data_in,cardinality,number_of_bytes,common::VARIANT);
      break;
    case common::BITPACKED:
      bitpacked::foreach(f,data_in,cardinality,number_of_bytes,common::BITPACKED);
      break;
      */
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
    case type::BITSET :
      bitset::foreach(f,data_in,cardinality,number_of_bytes,type::BITSET);
      break;
    /*
    case common::BITSET_NEW :
      bitset_new::foreach(f,data_in,cardinality,number_of_bytes,common::BITSET_NEW);
      break;
    case common::PSHORT :
      pshort::foreach(f,data_in,cardinality,number_of_bytes,common::PSHORT);
      break;
    case common::VARIANT :
      variant::foreach(f,data_in,cardinality,number_of_bytes,common::VARIANT);
      break;
    case common::BITPACKED :
      bitpacked::foreach(f,data_in,cardinality,number_of_bytes,common::BITPACKED);
      break;
      */
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
    case type::BITSET :
      return bitset::par_foreach(f,data_in,cardinality,number_of_bytes,type::BITSET);
      break;
      /*
    case common::PSHORT :
      return pshort::par_foreach(num_threads,f,data_in,cardinality,number_of_bytes,common::PSHORT);
      break;
    case common::VARIANT :
      std::cout << "Parallel foreach for VARIANT is not implemented" << std::endl;
      exit(EXIT_FAILURE);
      // variant::par_foreach(num_threads,f,data_in,cardinality,number_of_bytes,common::VARIANT);
      break;
    case common::BITPACKED :
      std::cout << "Parallel foreach for BITPACKED is not implemented" << std::endl;
      exit(EXIT_FAILURE);
      // bitpacked::par_foreach(num_threads,f,data_in,cardinality,number_of_bytes,common::BITPACKED);
      break;
    case common::BITSET_NEW :
      std::cout << "Parallel foreach for BITSET_NEW is not implemented" << std::endl;
      exit(EXIT_FAILURE);
      // bitpacked::par_foreach(num_threads,f,data_in,cardinality,number_of_bytes,common::BITPACKED);
      break;
    */
    default:
      return 0;
      break;
  }
}

#endif