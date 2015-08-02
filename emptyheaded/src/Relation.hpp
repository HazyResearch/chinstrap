/******************************************************************************
*
* Author: Christopher R. Aberger
*
* Stores a relation in a column wise fashion. Can take in any number of 
* different template arguments. Each template arguments corresponds to the
* type of the column. 
******************************************************************************/

#ifndef _RELATION_H_
#define _RELATION_H_

#include "Column.hpp"

// helpers
template <typename T>
struct id { using type = T; };

template <typename T>
using type_of = typename T::type;

template <size_t... N>
struct sizes : id <sizes <N...> > { };

// choose N-th element in list <T...>
template <size_t N, typename... T>
struct Choose;

template <size_t N, typename H, typename... T>
struct Choose <N, H, T...> : Choose <N-1, T...> { };

template <typename H, typename... T>
struct Choose <0, H, T...> : id <H> { };

template <size_t N, typename... T>
using choose = type_of <Choose <N, T...> >;

// given L>=0, generate sequence <0, ..., L-1>
template <size_t L, size_t I = 0, typename S = sizes <> >
struct Range;

template <size_t L, size_t I, size_t... N>
struct Range <L, I, sizes <N...> > : Range <L, I+1, sizes <N..., I> > { };

template <size_t L, size_t... N>
struct Range <L, L, sizes <N...> > : sizes <N...> { };

template <size_t L>
using range = type_of <Range <L> >;

// single Relation element
template <size_t N, typename T>
class RelationElem
{
  Column<T> elem;
public:
  Column<T>&       get()       { return elem; }
  const Column<T>& get() const { return elem; }
  RelationElem(){}

};

// Relation implementation
template <typename N, typename... T>
class RelationImpl;

template <size_t... N, typename... T>
class RelationImpl <sizes <N...>, T...> : RelationElem <N, T>...
{
  template <size_t M> using pick = choose <M, T...>;
  template <size_t M> using elem = RelationElem <M, pick <M> >;

public:
  template <size_t M>
  Column<pick <M>>& get() { return elem <M>::get(); }

  template <size_t M>
  const Column<pick <M>>& get() const { return elem <M>::get(); }
};


template <typename... T>
struct Relation : RelationImpl <range <sizeof...(T)>, T...>
{
  //return the number of columns (depends on the input thus a function)
  static constexpr std::size_t num_columns() { return sizeof...(T); }
  //stores the number of rows.
  size_t num_rows;
};

//Stores the dictionary encoded relation
struct EncodedRelation {
  size_t num_rows;
  size_t num_columns;
  std::vector<std::vector<uint32_t>> data;
  EncodedRelation(const size_t num_columns_in){
    num_rows = 0;
    num_columns = num_columns_in;
    for(size_t i = 0; i < num_columns; i++){
      data.push_back(std::vector<uint32_t>());
    }
  }
  //Return a pointer to the column of the encoded relation
  inline std::vector<uint32_t>* column(size_t i){
    return &data.at(i); 
  }
  //Append a value to a column.
  inline void append(const size_t col_index, const uint32_t value){
    data.at(col_index).push_back(value);
  }
};

#endif