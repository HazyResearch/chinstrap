#ifndef _ENCODING_H_
#define _ENCODING_H_

#include <unordered_map>
#include "Column.hpp"


template<class T>
struct OrderByFrequency{
  std::vector<size_t> *frequency;
  OrderByFrequency(std::vector<size_t> *frequency_in){
    frequency = frequency_in;
  }
  bool operator()(std::pair<uint32_t,T> i, std::pair<uint32_t,T> j) const {
    size_t i_size = frequency->at(i.first);
    size_t j_size = frequency->at(j.first);
    return i_size < j_size;
  }
};

template <class T>
struct Encoding{
  std::vector<Column<uint32_t>> encoded;
  std::unordered_map<T,uint32_t> value_to_key;
  std::vector<T> key_to_value;

  Encoding(std::vector<Column<T>> *attr_in){
    const size_t num_attributes = attr_in->size();

    //Get the frequency of each value
    std::vector<std::pair<uint32_t,T>> sorted_values;
    std::vector<size_t> frequency;
    for(size_t i = 0; i < num_attributes; i++){
      const Column<T> input = attr_in->at(i);
      for(size_t j = 0; j < input.size(); j++){
        const T value = input.at(j);
        if(!value_to_key.count(value)){
          value_to_key.insert(std::make_pair(value,sorted_values.size()));
          sorted_values.push_back(std::make_pair(sorted_values.size(),value));
          frequency.push_back(1);
        } else{
          frequency.at(value_to_key.at(value))++;
        }
      }
    }

    //sort by the frequency
    std::sort(sorted_values.begin(),sorted_values.end(),OrderByFrequency<T>(&frequency));

    //reassign id's
    key_to_value.reserve(frequency.size());
    for(size_t i = 0; i < sorted_values.size(); i++){
      value_to_key.at(sorted_values.at(i).second) = key_to_value.size();
      key_to_value.push_back(sorted_values.at(i).second);
    }

    //create new encoded column
    encoded.reserve(num_attributes);
    for(size_t i = 0; i < num_attributes; i++){
      const Column<T> input = attr_in->at(i);
      Column<uint32_t> output;
      output.reserve(input.size());
      for(size_t j = 0; j < input.size(); j++){
        const T value = input.at(j);
        const uint32_t key = value_to_key.at(value);
        output.append(key);
      }
      encoded.push_back(output);
    }

  };
};

#endif