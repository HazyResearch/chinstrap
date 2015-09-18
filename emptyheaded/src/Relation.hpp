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

#include "utils/utils.hpp"

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
  std::vector<T> elem;
public:
  std::vector<T>&       get()       { return elem; }
  const std::vector<T>& get() const { return elem; }
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
  std::vector<pick <M>>& get() { return elem <M>::get(); }

  template <size_t M>
  const std::vector<pick <M>>& get() const { return elem <M>::get(); }

  template <size_t M>
  pick <M> append_from_string(const char *string_element){
    const pick<M> value = utils::from_string<pick<M>>(string_element);
    elem <M>::get().push_back(value);
    return value;
  }

};


template <typename... T>
struct Relation : RelationImpl <range <sizeof...(T)>, T...>
{
  //return the number of columns (depends on the input thus a function)
  static constexpr std::size_t num_columns() { return sizeof...(T); }
  //stores the number of rows.
  size_t num_rows = 0;
};

template<class R>
struct EncodedRelation {
  std::vector<std::vector<uint32_t>> data;
  std::vector<uint32_t> max_set_size;
  std::vector<R> annotation;
  EncodedRelation(){}
  EncodedRelation(
    std::vector<std::vector<uint32_t>> data_in, 
    std::vector<uint32_t> num_distinct_in,
    std::vector<R> annotation_in){
    data = data_in;
    max_set_size = num_distinct_in;
    annotation = annotation_in;
  }

  std::vector<uint32_t>* column(size_t i){
    return &data.at(i);
  }

  void add_column(std::vector<uint32_t> *column_in,uint32_t num_distinct){
    data.push_back(*column_in);
    max_set_size.push_back(num_distinct);
  }

  void to_binary(std::string path){
    std::ofstream *writefile = new std::ofstream();
    std::string file = path+std::string("encoded.bin");
    writefile->open(file, std::ios::binary | std::ios::out);
    size_t num_columns = data.size();
    writefile->write((char *)&num_columns, sizeof(num_columns));
    for(size_t i = 0; i < data.size(); i++){
      const uint32_t mss = max_set_size.at(i);
      writefile->write((char *)&mss, sizeof(mss));
      const size_t num_rows = data.at(i).size();
      writefile->write((char *)&num_rows, sizeof(num_rows));
      for(size_t j = 0; j < data.at(i).size(); j++){
        writefile->write((char *)&data.at(i).at(j), sizeof(data.at(i).at(j)));
      }
    }
    const size_t num_rows = annotation.size();
    writefile->write((char *)&num_rows, sizeof(num_rows));
    for(size_t i = 0; i < annotation.size(); i++){
      const R aValue = annotation.at(i);
      writefile->write((char *)&aValue, sizeof(aValue));
    }
    writefile->close();
  }
  
  static EncodedRelation<R>* from_binary(std::string path){
    std::ifstream *infile = new std::ifstream();
    std::string file = path+std::string("encoded.bin");
    infile->open(file, std::ios::binary | std::ios::in);

    std::vector<std::vector<uint32_t>> data_in;

    size_t num_columns;
    infile->read((char *)&num_columns, sizeof(num_columns));
    data_in.resize(num_columns);

    std::vector<uint32_t> max_set_size_in;
    for(size_t i = 0; i < num_columns; i++){
      uint32_t mss;
      infile->read((char *)&mss, sizeof(mss));
      max_set_size_in.push_back(mss);

      size_t num_rows;
      infile->read((char *)&num_rows, sizeof(num_rows));
      std::vector<uint32_t>* new_column = new std::vector<uint32_t>();
      new_column->resize(num_rows);
      for(size_t j = 0; j < num_rows; j++){
        uint32_t value;
        infile->read((char *)&value, sizeof(value));
        new_column->at(j) = value;
      }
      data_in.at(i) = *new_column;
    }

    size_t num_annotation_rows;
    infile->read((char *)&num_annotation_rows, sizeof(num_annotation_rows));
    std::vector<R>* annotation_in = new std::vector<R>();
    annotation_in->resize(num_annotation_rows);
    for(size_t j = 0; j < num_annotation_rows; j++){
      R value;
      infile->read((char *)&value, sizeof(value));
      annotation_in->at(j) = value;
    }
    return new EncodedRelation<R>(data_in,max_set_size_in,*annotation_in);
  }

};

#endif