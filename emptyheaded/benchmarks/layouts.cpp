#include "emptyheaded.hpp"

int main (int argc, char* argv[]) {
  uint32_t a[10] = {0,1,2,3,4,5,257,512,768,2056};
  uint32_t b[10] = {0,1,2,3,4,5,256,512,768,1024};

  uint8_t *a_data = new uint8_t[10*10*sizeof(uint64_t)];
  uint8_t *b_data = new uint8_t[10*10*sizeof(uint64_t)];

  Set<block_bitset> as = Set<block_bitset>::from_array(a_data,a,10);
  Set<range_bitset> bs = Set<range_bitset>::from_array(b_data,b,10);

  uint8_t *c_data = new uint8_t[10*10*sizeof(uint64_t)];
  Set<block_bitset> cs(c_data);

  size_t count = ops::set_intersect(&cs,&as,&bs)->cardinality;
  std::cout << count << std::endl;

  return 0;
}