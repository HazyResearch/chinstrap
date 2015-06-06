#include "emptyheaded.hpp"

int main (int argc, char* argv[]) {
  uint32_t a[10] = {0,1,2,3,4,5,257,512,768,2056};
  uint32_t b[10] = {0,1,2,3,4,5,512,613,650,768};

  uint8_t *a_data = new uint8_t[10*10*sizeof(uint64_t)];
  uint8_t *b_data = new uint8_t[10*10*sizeof(uint64_t)];

  Set<uinteger> as = Set<uinteger>::from_array(a_data,a,10);
  Set<uinteger> bs = Set<uinteger>::from_array(b_data,b,10);

  uint8_t *c_data = new uint8_t[10*10*sizeof(uint64_t)];
  Set<uinteger> cs(c_data);

  size_t count = ops::set_intersect<materialize>(&cs,&as,&bs,[&](uint32_t data, uint32_t a_index, uint32_t b_index){
    std::cout << "Data: " << data << " a_i: " << a_index << " b_i: " << b_index << std::endl;
    return 1; 
  })->cardinality;
  std::cout << count << std::endl;

  cs.foreach([&](uint32_t data){
    std::cout << "Data: " << data << std::endl;
  });

  return 0;
}