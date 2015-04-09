#ifndef _ENCODING_H_
#define _ENCODING_H_

#include <unordered_map>
#include "Column.hpp"

template <class T>
class Encoding{
public:
  std::vector<Column<uint32_t>> encoded;
  std::unordered_map<T,uint32_t> key_to_value;
  std::vector<T> value_to_key;

  Encoding(std::vector<Column<T>> *attr_in){
    uint32_t index = 0;
    const size_t num_attributes = attr_in->size();
    for(size_t i = 0; i < num_attributes; i++){
      const Column<T> input = attr_in->at(i);
      Column<uint32_t> output;
      output.reserve(input.size());
      for(size_t j = 0; j < input.size(); j++){
        const uint32_t value = input.at(j);
        auto iter = key_to_value.find(value);
        uint32_t current_index = index;
        //We just found a new key, add it to the maps.
        if(iter == key_to_value.end()){
          value_to_key.push_back(value);
          key_to_value.insert(std::make_pair<T,uint32_t>(value,index++));
        } else{
          current_index = iter->second;
        }
        output.append(current_index);
      }
      encoded.push_back(output);
    }
  };
};

#endif