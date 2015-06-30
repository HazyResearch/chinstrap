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
    std::vector<Column<uint64_t>> *abcxyz_attributes =
        new std::vector<Column<uint64_t>>();
    abcxyz_attributes->push_back(R->get<0>());
    abcxyz_attributes->push_back(R->get<1>());
    Encoding<uint64_t> *abcxyz_encoding =
        new Encoding<uint64_t>(abcxyz_attributes);
    std::vector<Column<uint32_t>> *ER = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_R = new std::vector<void *>();
    ER->push_back(abcxyz_encoding->encoded.at(0));
    encodings_R->push_back((void *)abcxyz_encoding);
    ER->push_back(abcxyz_encoding->encoded.at(1));
    encodings_R->push_back((void *)abcxyz_encoding);
    Trie<hybrid> *TR = Trie<hybrid>::build(ER, [&](size_t index) {
      (void)index;
      return true;
    });
    tries["R"] = TR;
    encodings["R"] = encodings_R;
    allocator::memory<uint8_t> output_buffer(1 * 6 * 2 *
                                             sizeof(TrieBlock<hybrid>) *
                                             (abcxyz_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(1 * 6 * 2 *
                                          sizeof(TrieBlock<hybrid>) *
                                          (abcxyz_encoding->num_distinct));
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> R_abcxyz_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *R_abcxyz_block;
    {
      Set<hybrid> a;
      R_abcxyz_block = NULL;
      if (TR->head) {
        a = TR->head->set;
        R_abcxyz_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(TR->head);
        R_abcxyz_block->init_pointers(0, &output_buffer, a.cardinality,
                                      abcxyz_encoding->num_distinct,
                                      a.type == type::UINTEGER);
      }
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> b;
        TrieBlock<hybrid> *b_block = NULL;
        if (TR->head->get_block(a_d) && TR->head) {
          b_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>();
          const size_t alloc_size_b =
              sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
          b.data = output_buffer.get_next(
              tid, alloc_size_b); // initialize the memory
          b = *ops::set_intersect(
                  &b, (const Set<hybrid> *)&TR->head->get_block(a_d)->set,
                  (const Set<hybrid> *)&TR->head->set);
          output_buffer.roll_back(tid, alloc_size_b - b.number_of_bytes);
          b_block->set = &b;
          b_block->init_pointers(tid, &output_buffer, b.cardinality,
                                 abcxyz_encoding->num_distinct,
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
                sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
            c.data = output_buffer.get_next(
                tid, alloc_size_c); // initialize the memory
            c = *ops::set_intersect(
                    &c, (const Set<hybrid> *)&TR->head->get_block(b_d)->set,
                    (const Set<hybrid> *)&TR->head->get_block(a_d)->set);
            output_buffer.roll_back(tid, alloc_size_c - c.number_of_bytes);
            c_block->set = &c;
            c_block->init_pointers(tid, &output_buffer, c.cardinality,
                                   abcxyz_encoding->num_distinct,
                                   c.type == type::UINTEGER);
          }
          bool c_block_valid = false;
          c.foreach_index([&](uint32_t c_i, uint32_t c_d) {
            Set<hybrid> x;
            TrieBlock<hybrid> *x_block = NULL;
            if (TR->head && TR->head->get_block(a_d)) {
              x_block =
                  new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                      TrieBlock<hybrid>();
              const size_t alloc_size_x =
                  sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
              x.data = output_buffer.get_next(
                  tid, alloc_size_x); // initialize the memory
              x = *ops::set_intersect(
                      &x, (const Set<hybrid> *)&TR->head->set,
                      (const Set<hybrid> *)&TR->head->get_block(a_d)->set);
              output_buffer.roll_back(tid, alloc_size_x - x.number_of_bytes);
              x_block->set = &x;
              x_block->init_pointers(tid, &output_buffer, x.cardinality,
                                     abcxyz_encoding->num_distinct,
                                     x.type == type::UINTEGER);
            }
            bool x_block_valid = false;
            x.foreach_index([&](uint32_t x_i, uint32_t x_d) {
              Set<hybrid> y;
              TrieBlock<hybrid> *y_block = NULL;
              if (TR->head->get_block(x_d) && TR->head) {
                y_block =
                    new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>();
                const size_t alloc_size_y =
                    sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
                y.data = output_buffer.get_next(
                    tid, alloc_size_y); // initialize the memory
                y = *ops::set_intersect(
                        &y, (const Set<hybrid> *)&TR->head->get_block(x_d)->set,
                        (const Set<hybrid> *)&TR->head->set);
                output_buffer.roll_back(tid, alloc_size_y - y.number_of_bytes);
                y_block->set = &y;
                y_block->init_pointers(tid, &output_buffer, y.cardinality,
                                       abcxyz_encoding->num_distinct,
                                       y.type == type::UINTEGER);
              }
              bool y_block_valid = false;
              y.foreach_index([&](uint32_t y_i, uint32_t y_d) {
                Set<hybrid> z;
                TrieBlock<hybrid> *z_block = NULL;
                if (TR->head->get_block(y_d) && TR->head->get_block(x_d)) {
                  z_block = new (output_buffer.get_next(
                      tid, sizeof(TrieBlock<hybrid>))) TrieBlock<hybrid>();
                  const size_t alloc_size_z =
                      sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
                  z.data = output_buffer.get_next(
                      tid, alloc_size_z); // initialize the memory
                  z = *ops::set_intersect(
                          &z,
                          (const Set<hybrid> *)&TR->head->get_block(y_d)->set,
                          (const Set<hybrid> *)&TR->head->get_block(x_d)->set);
                  output_buffer.roll_back(tid,
                                          alloc_size_z - z.number_of_bytes);
                  z_block->set = &z;
                }
                if (z.cardinality != 0) {
                  const size_t count = z.cardinality;
                  R_abcxyz_cardinality.update(tid, count);
                  b_block_valid = true;
                  c_block_valid = true;
                  x_block_valid = true;
                  y_block_valid = true;
                  y_block->set_block(y_i, y_d, z_block);
                } else {
                  y_block->set_block(y_i, y_d, NULL);
                }
              });
              if (y_block_valid) {
                x_block->set_block(x_i, x_d, y_block);
              } else {
                x_block->set_block(x_i, x_d, NULL);
              }
            });
            if (x_block_valid) {
              c_block->set_block(c_i, c_d, x_block);
            } else {
              c_block->set_block(c_i, c_d, NULL);
            }
          });
          if (c_block_valid) {
            b_block->set_block(b_i, b_d, c_block);
          } else {
            b_block->set_block(b_i, b_d, NULL);
          }
        });
        if (b_block_valid) {
          R_abcxyz_block->set_block(a_i, a_d, b_block);
        } else {
          R_abcxyz_block->set_block(a_i, a_d, NULL);
        }
      });
    }
    std::cout << R_abcxyz_cardinality.evaluate(0) << std::endl;
    Trie<hybrid> *TRESULT = new Trie<hybrid>(R_abcxyz_block);
    tries["RESULT"] = TRESULT;
    std::vector<void *> *encodings_RESULT = new std::vector<void *>();
    encodings_RESULT->push_back(abcxyz_encoding);
    encodings_RESULT->push_back(abcxyz_encoding);
    encodings_RESULT->push_back(abcxyz_encoding);
    encodings_RESULT->push_back(abcxyz_encoding);
    encodings_RESULT->push_back(abcxyz_encoding);
    encodings_RESULT->push_back(abcxyz_encoding);
    encodings["RESULT"] = encodings_RESULT;
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
