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
      return true;
    });
    tries["R"] = TR;
    encodings["R"] = encodings_R;
    allocator::memory<uint8_t> output_buffer(
        1 * 3 * 2 * sizeof(TrieBlock<hybrid>) * (abc_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        1 * 3 * 2 * sizeof(TrieBlock<hybrid>) * (abc_encoding->num_distinct));
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> R_abc_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *R_abc_block;
    {
      Set<hybrid> a;
      R_abc_block = NULL;
      if (TR->head) {
        a = TR->head->set;
        R_abc_block = new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
            TrieBlock<hybrid>(TR->head);
        R_abc_block->init_pointers(0, &output_buffer, a.cardinality,
                                   abc_encoding->num_distinct,
                                   a.type == type::UINTEGER);
      }
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> b;
        TrieBlock<hybrid> *b_block = NULL;
        if (TR->head->get_block(a_d) && TR->head) {
          b_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>();
          const size_t alloc_size_b =
              sizeof(uint64_t) * abc_encoding->num_distinct * 2;
          b.data = output_buffer.get_next(
              tid, alloc_size_b); // initialize the memory
          b = *ops::set_intersect(
                  &b, (const Set<hybrid> *)&TR->head->get_block(a_d)->set,
                  (const Set<hybrid> *)&TR->head->set);
          output_buffer.roll_back(tid, alloc_size_b - b.number_of_bytes);
          b_block->set = &b;
          b_block->init_pointers(tid, &output_buffer, b.cardinality,
                                 abc_encoding->num_distinct,
                                 b.type == type::UINTEGER);
        }
        bool b_block_valid = false;
        b.foreach_index([&](uint32_t b_i, uint32_t b_d) {
          Set<hybrid> c;
          TrieBlock<hybrid> *c_block = NULL;
          if (TR->head->get_block(b_d) && TR->head->get_block(a_d)) {
            c_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>();
            const size_t alloc_size_c =
                sizeof(uint64_t) * abc_encoding->num_distinct * 2;
            c.data = output_buffer.get_next(
                tid, alloc_size_c); // initialize the memory
            c = *ops::set_intersect(
                    &c, (const Set<hybrid> *)&TR->head->get_block(b_d)->set,
                    (const Set<hybrid> *)&TR->head->get_block(a_d)->set);
            output_buffer.roll_back(tid, alloc_size_c - c.number_of_bytes);
            c_block->set = &c;
          }
          if (c.cardinality != 0) {
            const size_t count = c.cardinality;
            R_abc_cardinality.update(tid, count);
            b_block_valid = true;
            b_block->set_block(b_i, b_d, c_block);
          } else {
            b_block->set_block(b_i, b_d, NULL);
          }
        });
        if (b_block_valid) {
          R_abc_block->set_block(a_i, a_d, b_block);
        } else {
          R_abc_block->set_block(a_i, a_d, NULL);
        }
      });
    }
    std::cout << R_abc_cardinality.evaluate(0) << std::endl;
    Trie<hybrid> *TU = new Trie<hybrid>(R_abc_block);
    tries["U"] = TU;
    std::vector<void *> *encodings_U = new std::vector<void *>();
    encodings_U->push_back(abc_encoding);
    encodings_U->push_back(abc_encoding);
    encodings_U->push_back(abc_encoding);
    encodings["U"] = encodings_U;
    debug::stop_clock("JOIN", join_timer);
    tmp_buffer.free();
  }
}
int main() {
  std::unordered_map<std::string, void *> relations;
  std::unordered_map<std::string, Trie<hybrid> *> tries;
  std::unordered_map<std::string, std::vector<void *> *> encodings;
  run(relations, tries, encodings);
}
