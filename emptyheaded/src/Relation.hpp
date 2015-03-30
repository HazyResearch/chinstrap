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
  Column<T>* elem;
public:
  Column<T>*&       get()       { return elem; }
  const Column<T>*& get() const { return elem; }
  
  RelationElem(){
    elem = new Column<T>();
  }

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
  Column<pick <M>>*& get() { return elem <M>::get(); }

  template <size_t M>
  const Column<pick <M>>*& get() const { return elem <M>::get(); }
};


template <typename... T>
struct Relation : RelationImpl <range <sizeof...(T)>, T...>
{
  static constexpr std::size_t num_rows() { return sizeof...(T); }
  size_t num_columns;

  /*
  // primary template
  template <int DIM, typename F>
  class ForeachRow {
    public:
      static void result(F f) {
        f(DIM);
        ForeachRow<DIM+1,F>::result(f);
      }
  };

  // partial specialization as end criteria
  template <typename F>
  class ForeachRow<num_rows()-1,F> {
    public:
      static void result (F f) {
        f(0);
        //return *a * *b;
      }
  };

  template<typename F>
  inline void foreach_row(F f){
    ForeachRow<0,F>::result(f);
  }
  */
};


#endif