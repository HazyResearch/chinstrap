/*

THIS CLASS IMPLEMENTS THE FUNCTIONS ASSOCIATED WITH AN UNCOMPRESSED SET LAYOUT.
QUITE SIMPLE THE LAYOUT JUST CONTAINS UNSIGNED INTEGERS IN THE SET.

*/

#include "utils/utils.hpp"

class uinteger{
  public:
    static common::type get_type();
    static size_t build(uint8_t *r_in, const uint32_t *data, const size_t length);
    static size_t build_flattened(uint8_t *r_in, const uint32_t *data, const size_t length);
    static std::tuple<size_t,size_t,common::type> get_flattened_data(const uint8_t *set_data, const size_t cardinality);

    template<typename F>
    static void foreach(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const common::type t);

    template<typename F>
    static void foreach_until(
        F f,
        const uint8_t *data_in,
        const size_t cardinality,
        const size_t number_of_bytes,
        const common::type t);

    template<typename F>
    static size_t par_foreach(
      F f,
      const uint8_t *data_in,
      const size_t cardinality,
      const size_t number_of_bytes,
      const common::type t);

    static std::tuple<size_t,size_t,common::type> intersect(uint8_t *C_in, const uint8_t *A_in, const uint8_t *B_in, const size_t A_cardinality, const size_t B_cardinality, const size_t A_num_bytes, const size_t B_num_bytes, const common::type a_t, const common::type b_t);
};

inline common::type uinteger::get_type(){
  return common::UINTEGER;
}
//Copies data from input array of ints to our set data r_in
inline size_t uinteger::build(uint8_t *r_in, const uint32_t *data, const size_t length){
  uint32_t *r = (uint32_t*) r_in;
  std::copy(data,data+length,r);
  return length*sizeof(uint32_t);
}
//Nothing is different about build flattened here. The number of bytes
//can be infered from the type. This gives us back a true CSR representation.
inline size_t uinteger::build_flattened(uint8_t *r_in, const uint32_t *data, const size_t length){
  common::num_uint++;
  return build(r_in,data,length);
}

inline std::tuple<size_t,size_t,common::type> uinteger::get_flattened_data(const uint8_t *set_data, const size_t cardinality){
  (void) set_data;
  return std::make_tuple(0,cardinality*sizeof(uint32_t),common::UINTEGER);
}

//Iterates over set applying a lambda.
template<typename F>
inline void uinteger::foreach(
    F f,
    const uint8_t *data_in,
    const size_t cardinality,
    const size_t number_of_bytes,
    const common::type t) {
 (void) number_of_bytes; (void) t;

 uint32_t *data = (uint32_t*) data_in;
 for(size_t i=0; i<cardinality;i++){
  f(data[i]);
 }
}

//Iterates over set applying a lambda.
template<typename F>
inline void uinteger::foreach_until(
    F f,
    const uint8_t *data_in,
    const size_t cardinality,
    const size_t number_of_bytes,
    const common::type t) {
 (void) number_of_bytes; (void) t;

  uint32_t *data = (uint32_t*) data_in;
  for(size_t i=0; i<cardinality;i++){
    if(f(data[i]))
      break;
  }
}

// Iterates over set applying a lambda in parallel.
template<typename F>
inline size_t uinteger::par_foreach(
      F f,
      const uint8_t *data_in,
      const size_t cardinality,
      const size_t number_of_bytes,
      const common::type t) {
   (void) number_of_bytes; (void) t;

   uint32_t* data = (uint32_t*) data_in;
   return par::for_range(0, cardinality, 64,
     [&f, &data](size_t tid, size_t i) {
        f(tid, data[i]);
     });
}
