#ifndef _SET_H_
#define _SET_H_

/*
TOP LEVEL CLASS FOR OUR SORTED SETS.  PROVIDE A SET OF GENERAL PRIMITIVE 
TYPES THAT WE PROVIDE (PSHORT,UINT,VARIANT,BITPACKED,BITSET).  WE ALSO
PROVIDE A HYBRID SET IMPLEMENTATION THAT DYNAMICALLY CHOOSES THE TYPE
FOR THE DEVELOPER. IMPLEMENTATION FOR PRIMITIVE TYPE OPS CAN BE 
FOUND IN STATIC CLASSES IN THE LAYOUT FOLDER.
*/

#include "layouts/hybrid.hpp"
//#include "layouts/blockl.hpp"

template <class T>
class Set{ 
  public: 
    uint8_t *data;
    size_t cardinality;
    size_t number_of_bytes;
    double density;
    type::layout type;

    Set(){
      number_of_bytes = 0;
      cardinality = 0;
    };
    //All values passed in
    Set(uint8_t *data_in, 
      size_t cardinality_in, 
      size_t number_of_bytes_in,
      double density_in,
      type::layout type_in):
      data(data_in),
      cardinality(cardinality_in),
      number_of_bytes(number_of_bytes_in),
      density(density_in),
      type(type_in){}

    //Density is optional in read-only sets
    Set(uint8_t *data_in, 
      size_t cardinality_in, 
      size_t number_of_bytes_in,
      type::layout type_in):
      data(data_in),
      cardinality(cardinality_in),
      number_of_bytes(number_of_bytes_in),
      type(type_in){
        density = 0.0;
      }

    //A set that is just a buffer zeroed out to certain size.
    Set(size_t number_of_bytes_in){
        cardinality = 0;
        number_of_bytes = number_of_bytes_in;
        density = 0.0;
        type = T::get_type();

        data = new uint8_t[number_of_bytes];
        memset(data,(uint8_t)0,number_of_bytes);
      }

    //A set that is just a buffer.
    Set(uint8_t *data_in):
      data(data_in){
        cardinality = 0;
        number_of_bytes = 0;
        density = 0.0;
        type = T::get_type();
      }

    //Implicit Conversion Between Unlike Types
    template <class U> 
    Set<T>(Set<U> in){
      data = in.data;
      cardinality = in.cardinality;
      number_of_bytes = in.number_of_bytes;
      density = in.density;
      type = in.type;
    }

    Set(uint8_t* data_in, size_t number_of_bytes_in):
      data(data_in), number_of_bytes(number_of_bytes_in) {
        cardinality = 0;
        number_of_bytes = number_of_bytes_in;
        density = 0.0;
        type = T::get_type();
    }

    template <class U> 
    Set<T>(Set<U> *in){
      data = in->data;
      cardinality = in->cardinality;
      number_of_bytes = in->number_of_bytes;
      density = in->density;
      type = in->type;
    }

    // Applies a function to each element in the set.
    //
    // Note: We use templates here to allow the compiler to inline the
    // lambda [1].
    //
    // [1] http://stackoverflow.com/questions/21407691/c11-inline-lambda-functions-without-template
    template<typename F>
    void foreach(F f) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      T::foreach(f,data,cardinality,number_of_bytes,type);
    }

    template<typename F>
    void foreach_index(F f) const {
      /*std::cout << number_of_bytes << std::endl;
      std::cout << number_of_bytes << std::endl;*/
      //std::cout << "---" << std::endl;
      T::foreach_index(f,data,cardinality,number_of_bytes,type);
    }

    // Applies a function to each element in the set until the function returns
    // true.
    //
    // Note: We use templates here to allow the compiler to inline the
    // lambda [1].
    //
    // [1] http://stackoverflow.com/questions/21407691/c11-inline-lambda-functions-without-template
    template<typename F>
    void foreach_until(F f) const {
      T::foreach_until(f,data,cardinality,number_of_bytes,type);
    }

    template<typename F>
    size_t par_foreach(F f) const {
      return T::par_foreach(f, data, cardinality, number_of_bytes, type);
    }

    template<typename F>
    size_t par_foreach_index(F f) const {
      return T::par_foreach_index(f, data, cardinality, number_of_bytes, type);
    }

    Set<uinteger> decode(uint32_t *buffer);
    void copy_from(Set<T> src);

    //Constructors
    static Set<T> from_array(uint8_t *set_data, uint32_t *array_data, size_t data_size);
    static Set<T> from_flattened(uint8_t *set_data, size_t cardinality_in);
    static size_t flatten_from_array(uint8_t *set_data, const uint32_t * const array_data, const size_t data_size);
};

///////////////////////////////////////////////////////////////////////////////
//Copy Data from one set into another
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline void Set<T>::copy_from(Set<T> src){ 
  memcpy(data,src.data,src.number_of_bytes);
  cardinality = src.cardinality;
  number_of_bytes = src.number_of_bytes;
  density = src.density;
  type = src.type;
}
///////////////////////////////////////////////////////////////////////////////
//DECODE ARRAY
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline Set<uinteger> Set<T>::decode(uint32_t *buffer){ 
  size_t i = 0;
  T::foreach( ([&i,&buffer] (uint32_t data){
    buffer[i++] = data;
  }),data,cardinality,number_of_bytes,type);
  return Set<uinteger>((uint8_t*)buffer,cardinality,cardinality*sizeof(int),density,type::UINTEGER);
}

///////////////////////////////////////////////////////////////////////////////
//CREATE A SET FROM AN ARRAY OF UNSIGNED INTEGERS
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline Set<T> Set<T>::from_array(uint8_t *set_data, uint32_t *array_data, size_t data_size){
  const double density = ((data_size > 0) ? (double)((array_data[data_size-1]-array_data[0])/data_size) : 0.0);
  const std::tuple<size_t,type::layout> bl = T::build(set_data,array_data,data_size);
  return Set<T>(set_data,data_size,std::get<0>(bl),density,std::get<1>(bl));
}

///////////////////////////////////////////////////////////////////////////////
//CREATE A SET FROM A FLATTENED ARRAY WITH INFO.  
//THIS IS USED FOR A CSR GRAPH IMPLEMENTATION.
//Assume these are already packaged and re-optimized, density is not adjusted
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline Set<T> Set<T>::from_flattened(uint8_t *set_data, size_t cardinality_in){
  auto flattened_data = T::get_flattened_data(set_data,cardinality_in);
  return Set<T>(&set_data[std::get<0>(flattened_data)],cardinality_in,std::get<1>(flattened_data),std::get<2>(flattened_data));
}

///////////////////////////////////////////////////////////////////////////////
//FLATTEN A SET INTO AN ARRAY.  
//THIS IS USED FOR A CSR GRAPH IMPLEMENTATION.
///////////////////////////////////////////////////////////////////////////////
template <class T>
inline size_t Set<T>::flatten_from_array(uint8_t *set_data, const uint32_t * const array_data, const size_t data_size){
  return T::build_flattened(set_data,array_data,data_size);
}

#endif
