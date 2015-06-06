#include "emptyheaded.hpp"

int main (int argc, char* argv[]) {

  const size_t a_size = 3000;
  uint32_t *a = new uint32_t[a_size];
  for(size_t i = 0; i < a_size; i++){
    a[i] = i;
  }

  uint32_t b[10] = {0,50,78,89,90,91,96,613,650,768};

  uint8_t *a_data = new uint8_t[a_size*10*sizeof(uint64_t)];
  uint8_t *b_data = new uint8_t[10*10*sizeof(uint64_t)];

  Set<uinteger> as = Set<uinteger>::from_array(a_data,a,a_size);
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