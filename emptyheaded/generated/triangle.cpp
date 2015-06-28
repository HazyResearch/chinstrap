#include "emptyheaded.hpp"
extern "C" void
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<uint64_t, uint64_t> *R = new Relation<uint64_t, uint64_t>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/datasets/facebook/edgelist/replicated.tsv");
    char *next = f_reader.tsv_get_first();
    R->num_rows = 0;
    while (next != NULL) {
      R->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      R->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      R->num_rows++;
    }
    relations["R"] = R;
    std::cout << R->num_rows << " rows loaded." << std::endl;
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<uint64_t, uint64_t> *R =
        (Relation<uint64_t, uint64_t> *)relations["R"];
    std::vector<Column<uint64_t>> *abc_attributes =
        new std::vector<Column<uint64_t>>();
    abc_attributes->push_back(R->get<0>());
    abc_attributes->push_back(R->get<1>());
    Encoding<uint64_t> *abc_encoding = new Encoding<uint64_t>(abc_attributes);
    std::vector<Column<uint32_t>> *ER = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_R = new std::vector<void *>();
    ER->push_back(abc_encoding->encoded.at(0));
    encodings_R->push_back((void *)abc_encoding);
    ER->push_back(abc_encoding->encoded.at(1));
    encodings_R->push_back((void *)abc_encoding);
    Trie<hybrid> *TR = Trie<hybrid>::build(ER, [&](size_t index) {
      (void)index;
      return true;//ER->at(0).at(index) > ER->at(1).at(index);
    });
    tries["R"] = TR;
    encodings["R"] = encodings_R;
    allocator::memory<uint8_t> output_buffer(
        10000 * 3 * 2 * sizeof(TrieBlock<hybrid>) * (abc_encoding->num_distinct));
    auto a1 = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> R_abc_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *R_abc_block;
    {
      Set<hybrid> a = TR->head->set;
      R_abc_block = new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
          TrieBlock<hybrid>(TR->head);
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        TrieBlock<hybrid> *b_block =
            new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size =
            sizeof(uint64_t) * abc_encoding->num_distinct * 2;
        Set<hybrid> b(
            output_buffer.get_next(tid, alloc_size)); // initialize the memory
        b = *ops::set_intersect(
                &b, (const Set<hybrid> *)&TR->head->get_block(a_d)->set,
                (const Set<hybrid> *)&TR->head->set);
        output_buffer.roll_back(tid, alloc_size - b.number_of_bytes);
        b_block->set = &b;
        b_block->init_pointers(tid, &output_buffer, b.cardinality,
                               abc_encoding->num_distinct,
                               b.type == type::UINTEGER);
        if (b.cardinality != 0)
          R_abc_block->set_block(a_i, a_d, b_block);
        b.foreach_index([&](uint32_t b_i, uint32_t b_d) {
          TrieBlock<hybrid> *c_block =
              new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                  TrieBlock<hybrid>();
          const size_t alloc_size =
              sizeof(uint64_t) * abc_encoding->num_distinct * 2;
          Set<hybrid> c(
              output_buffer.get_next(tid, alloc_size)); // initialize the memory
          c = *ops::set_intersect(
                  &c, (const Set<hybrid> *)&TR->head->get_block(b_d)->set,
                  (const Set<hybrid> *)&TR->head->get_block(a_d)->set);
          output_buffer.roll_back(tid, alloc_size - c.number_of_bytes);
          c_block->set = &c;
          c_block->init_pointers(tid, &output_buffer, c.cardinality,
                                 abc_encoding->num_distinct,
                                 c.type == type::UINTEGER);
          if (c.cardinality != 0)
            b_block->set_block(b_i, b_d, c_block);
          const size_t count = c.cardinality;
          R_abc_cardinality.update(tid, count);
        });
      });
    }
    debug::stop_clock("JOIN",a1);
    std::cout << R_abc_cardinality.evaluate(0) << std::endl;
    Trie<hybrid> *TU = new Trie<hybrid>(R_abc_block);
    tries["U"] = TU;
    std::vector<void *> *encodings_U = new std::vector<void *>();
    encodings_U->push_back(abc_encoding);
    encodings_U->push_back(abc_encoding);
    encodings_U->push_back(abc_encoding);
    encodings["U"] = encodings_U;
  }
}
int main() {
  std::unordered_map<std::string, void *> relations;
  std::unordered_map<std::string, Trie<hybrid> *> tries;
  std::unordered_map<std::string, std::vector<void *> *> encodings;
  run(relations, tries, encodings);
}
