#include <iostream>
#include <unordered_map>
#include "emptyheaded.hpp"
#include "utils/io.hpp"
extern "C" void run(std::unordered_map<std::string, void*>& relations) {
  Relation<uint64_t,uint64_t>* R = new Relation<uint64_t,uint64_t>();
  tsv_reader f_reader("data/small.txt");
  char *next = f_reader.tsv_get_first();
  R->num_rows = 0;
  while(next != NULL){
    R->get<0>().append_from_string(next);
    next = f_reader.tsv_get_next();
    R->get<1>().append_from_string(next);
    next = f_reader.tsv_get_next();
    R->num_rows++;
  }

  Relation<uint64_t, uint64_t> * R= (Relation<uint64_t, uint64_t> *)relations["R"];
  std::vector<Column<uint64_t>> *R_0_attributes = new std::vector<Column<uint64_t>>();
  R_0_attributes->push_back(R->get<0>());
  R_0_attributes->push_back(R->get<1>());
  Encoding<uint64_t> R_0_encoding(R_0_attributes); // TODO heap allocate
  std::vector<Column<uint32_t>> *ER = new std::vector<Column<uint32_t>>();
  ER->push_back(R_0_encoding.encoded.at(0));
  ER->push_back(R_0_encoding.encoded.at(1));
  Trie<hybrid> *TR = Trie<hybrid>::build(ER, [&](size_t index){return true;});
  allocator::memory<uint8_t> a_buffer(R_0_encoding.key_to_value.size());
  allocator::memory<uint8_t> b_buffer(R_0_encoding.key_to_value.size());
  allocator::memory<uint8_t> c_buffer(R_0_encoding.key_to_value.size());
  par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){
    return a + b;
  });

  Set<hybrid> a = TR->head->data;
  a.par_foreach([&](size_t tid, uint32_t a_i){
    Set<hybrid> b(b_buffer.get_memory(tid)); //initialize the memory
    b = ops::set_intersect(&b,&TR->head->get_block(a_i)->data,&TR->head->data);
    b.foreach([&](uint32_t b_i) {
      Set<hybrid> c(c_buffer.get_memory(tid)); //initialize the memory
      c = ops::set_intersect(&c,&TR->head->get_block(b_i)->data,&TR->head->get_block(a_i)->data);
      const size_t count = c.cardinality;
      num_triangles.update(tid,count);
    });
  });
  size_t result = num_triangles.evaluate(0);
  std::cout << result << std::endl;
}

int main() {
  std::unordered_map<std::string, void*> relations;
  run(relations);
}
