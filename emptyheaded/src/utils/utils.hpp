#ifndef UTILS_H
#define UTILS_H

#include "io.hpp"
#include "parallel.hpp"
#include "allocator.hpp"
//#include "pcm_helper.hpp"
#include "debug.hpp"

template<typename F>
long binary_search(const uint32_t * const data, size_t first, size_t last, uint32_t search_key, F f){
 long index;
 if (first > last)
  index = -1;
 
 else{
  size_t mid = (last+first)/2;
  if (search_key == data[f(mid)])
    index = mid;
  else{
    if (search_key < data[f(mid)])
      index = binary_search(data,first, mid-1,search_key,f);
    else
      index = binary_search(data,mid+1,last,search_key,f);
  }
 } // end if
 return index;
}// end binarySearch

#endif
