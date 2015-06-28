#include "emptyheaded.hpp"
extern "C" void
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<uint64_t, uint64_t> *R = new Relation<uint64_t, uint64_t>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/chinstrap/duncecap/data/1_barbell.txt");
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
    allocator::memory<uint8_t> output_buffer(3 * 6 * 2 *
                                             sizeof(TrieBlock<hybrid>) *
                                             (abcxyz_encoding->num_distinct));
    //////////NPRR
    par::reducer<size_t> R_acb_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *R_acb_block;
    {
      Set<hybrid> a = TR->head->set;
      R_acb_block = new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
          TrieBlock<hybrid>(TR->head);
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        TrieBlock<hybrid> *c_block =
            new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size =
            sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
        Set<hybrid> c(
            output_buffer.get_next(tid, alloc_size)); // initialize the memory
        c = *ops::set_intersect(
                &c, (const Set<hybrid> *)&TR->head->get_block(a_d)->set,
                (const Set<hybrid> *)&TR->head->set);
        output_buffer.roll_back(tid, alloc_size - c.number_of_bytes);
        c_block->set = &c;
        c_block->init_pointers(tid, &output_buffer, c.cardinality,
                               abcxyz_encoding->num_distinct,
                               c.type == type::UINTEGER);
        R_acb_block->set_block(a_i, a_d, c_block);
        c.foreach_index([&](uint32_t c_i, uint32_t c_d) {
          TrieBlock<hybrid> *b_block =
              new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                  TrieBlock<hybrid>();
          const size_t alloc_size =
              sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
          Set<hybrid> b(
              output_buffer.get_next(tid, alloc_size)); // initialize the memory
          b = *ops::set_intersect(
                  &b, (const Set<hybrid> *)&TR->head->set,
                  (const Set<hybrid> *)&TR->head->get_block(a_d)->set);
          output_buffer.roll_back(tid, alloc_size - b.number_of_bytes);
          b_block->set = &b;
          b_block->init_pointers(tid, &output_buffer, b.cardinality,
                                 abcxyz_encoding->num_distinct,
                                 b.type == type::UINTEGER);
          c_block->set_block(c_i, c_d, b_block);
          const size_t count = b.cardinality;
          R_acb_cardinality.update(tid, count);
        });
      });
    }
    std::cout << R_acb_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> R_xyz_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *R_xyz_block;
    {
      Set<hybrid> x = TR->head->set;
      R_xyz_block = new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
          TrieBlock<hybrid>(TR->head);
      x.par_foreach_index([&](size_t tid, uint32_t x_i, uint32_t x_d) {
        TrieBlock<hybrid> *y_block =
            new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size =
            sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
        Set<hybrid> y(
            output_buffer.get_next(tid, alloc_size)); // initialize the memory
        y = *ops::set_intersect(
                &y, (const Set<hybrid> *)&TR->head->get_block(x_d)->set,
                (const Set<hybrid> *)&TR->head->set);
        output_buffer.roll_back(tid, alloc_size - y.number_of_bytes);
        y_block->set = &y;
        y_block->init_pointers(tid, &output_buffer, y.cardinality,
                               abcxyz_encoding->num_distinct,
                               y.type == type::UINTEGER);
        R_xyz_block->set_block(x_i, x_d, y_block);
        y.foreach_index([&](uint32_t y_i, uint32_t y_d) {
          TrieBlock<hybrid> *z_block =
              new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                  TrieBlock<hybrid>();
          const size_t alloc_size =
              sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
          Set<hybrid> z(
              output_buffer.get_next(tid, alloc_size)); // initialize the memory
          z = *ops::set_intersect(
                  &z, (const Set<hybrid> *)&TR->head->get_block(y_d)->set,
                  (const Set<hybrid> *)&TR->head->get_block(x_d)->set);
          output_buffer.roll_back(tid, alloc_size - z.number_of_bytes);
          z_block->set = &z;
          z_block->init_pointers(tid, &output_buffer, z.cardinality,
                                 abcxyz_encoding->num_distinct,
                                 z.type == type::UINTEGER);
          y_block->set_block(y_i, y_d, z_block);
          const size_t count = z.cardinality;
          R_xyz_cardinality.update(tid, count);
        });
      });
    }
    std::cout << R_xyz_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> R_ax_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *R_ax_block;
    {
      R_ax_block = new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
          TrieBlock<hybrid>();
      const size_t alloc_size =
          sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
      Set<hybrid> a(
          output_buffer.get_next(0, alloc_size)); // initialize the memory
      a = *ops::set_intersect(&a, (const Set<hybrid> *)&TR->head->set,
                              (const Set<hybrid> *)&R_acb_block->set);
      output_buffer.roll_back(0, alloc_size - a.number_of_bytes);
      R_ax_block->set = &a;
      R_ax_block->init_pointers(0, &output_buffer, a.cardinality,
                                abcxyz_encoding->num_distinct,
                                a.type == type::UINTEGER);
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        TrieBlock<hybrid> *x_block =
            new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size =
            sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
        Set<hybrid> x(
            output_buffer.get_next(tid, alloc_size)); // initialize the memory
        x = *ops::set_intersect(
                &x, (const Set<hybrid> *)&TR->head->get_block(a_d)->set,
                (const Set<hybrid> *)&R_xyz_block->set);
        output_buffer.roll_back(tid, alloc_size - x.number_of_bytes);
        x_block->set = &x;
        x_block->init_pointers(tid, &output_buffer, x.cardinality,
                               abcxyz_encoding->num_distinct,
                               x.type == type::UINTEGER);
        R_ax_block->set_block(a_i, a_d, x_block);
        const size_t count = x.cardinality;
        R_ax_cardinality.update(tid, count);
      });
    }
    std::cout << R_ax_cardinality.evaluate(0) << std::endl;
    ///////////////////TOP DOWN
    par::reducer<size_t> RESULT_abcxyz_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *RESULT_abcxyz_block;
    {
      RESULT_abcxyz_block = R_ax_block;
      if (RESULT_abcxyz_block) {
        RESULT_abcxyz_block->set.par_foreach_index([&](size_t tid, uint32_t a_i,
                                                       uint32_t a_d) {
          TrieBlock<hybrid> *x_block = R_ax_block->get_block(a_d);
          TrieBlock<hybrid> *c_block = R_acb_block->get_block(a_d);
          if (x_block && c_block) {
            x_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>(x_block);
            RESULT_abcxyz_block->set_block(a_i, a_d, x_block);
            x_block->init_pointers(tid, &output_buffer,
                                   x_block->set.cardinality,
                                   abcxyz_encoding->num_distinct,
                                   x_block->set.type == type::UINTEGER);
            x_block->set.foreach_index([&](uint32_t x_i, uint32_t x_d) {
              TrieBlock<hybrid> *y_block = R_xyz_block->get_block(x_d);
              if (c_block && y_block) {
                c_block =
                    new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(c_block);
                x_block->set_block(x_i, x_d, c_block);
                c_block->init_pointers(tid, &output_buffer,
                                       c_block->set.cardinality,
                                       abcxyz_encoding->num_distinct,
                                       c_block->set.type == type::UINTEGER);
                c_block->set.foreach_index([&](uint32_t c_i, uint32_t c_d) {
                  TrieBlock<hybrid> *b_block =
                      R_acb_block->get_block(a_d)->get_block(c_d);
                  if (b_block) {
                    b_block = new (
                        output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(b_block);
                    c_block->set_block(c_i, c_d, b_block);
                    b_block->init_pointers(tid, &output_buffer,
                                           b_block->set.cardinality,
                                           abcxyz_encoding->num_distinct,
                                           b_block->set.type == type::UINTEGER);
                    b_block->set.foreach_index([&](uint32_t b_i, uint32_t b_d) {
                      if (y_block) {
                        y_block = new (output_buffer.get_next(
                            tid, sizeof(TrieBlock<hybrid>)))
                            TrieBlock<hybrid>(y_block);
                        b_block->set_block(b_i, b_d, y_block);
                        y_block->init_pointers(
                            tid, &output_buffer, y_block->set.cardinality,
                            abcxyz_encoding->num_distinct,
                            y_block->set.type == type::UINTEGER);
                        y_block->set.foreach_index(
                            [&](uint32_t y_i, uint32_t y_d) {
                              TrieBlock<hybrid> *z_block =
                                  R_xyz_block->get_block(x_d)->get_block(y_d);
                              if (z_block) {
                                z_block = new (output_buffer.get_next(
                                    tid, sizeof(TrieBlock<hybrid>)))
                                    TrieBlock<hybrid>(z_block);
                                y_block->set_block(y_i, y_d, z_block);
                                const size_t count = z_block->set.cardinality;
                                RESULT_abcxyz_cardinality.update(tid, count);
                              }
                            });
                      }
                    });
                  }
                });
              }
            });
          }
        });
      }
      Trie<hybrid> *TRESULT = new Trie<hybrid>(RESULT_abcxyz_block);
      tries["RESULT"] = TRESULT;
      std::vector<void *> *encodings_RESULT = new std::vector<void *>();
      encodings_RESULT->push_back(abcxyz_encoding);
      encodings_RESULT->push_back(abcxyz_encoding);
      encodings_RESULT->push_back(abcxyz_encoding);
      encodings_RESULT->push_back(abcxyz_encoding);
      encodings_RESULT->push_back(abcxyz_encoding);
      encodings_RESULT->push_back(abcxyz_encoding);
      encodings["RESULT"] = encodings_RESULT;
    }
    std::cout << RESULT_abcxyz_cardinality.evaluate(0) << std::endl;
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Trie<hybrid> *TRESULT = tries["RESULT"];
    std::vector<void *> *encodings_RESULT = encodings["RESULT"];
    TRESULT->head->set.foreach_index([&](uint32_t a_i, uint32_t a_d) {
      (void)a_i;
      TRESULT->head->get_block(a_d)->set.foreach_index([&](uint32_t b_i,
                                                           uint32_t b_d) {
        (void)b_i;
        TRESULT->head->get_block(a_d)->get_block(b_d)->set.foreach_index([&](
            uint32_t c_i, uint32_t c_d) {
          (void)c_i;
          TRESULT->head->get_block(a_d)
              ->get_block(b_d)
              ->get_block(c_d)
              ->set.foreach_index([&](uint32_t x_i, uint32_t x_d) {
                (void)x_i;
                TRESULT->head->get_block(a_d)
                    ->get_block(b_d)
                    ->get_block(c_d)
                    ->get_block(x_d)
                    ->set.foreach_index([&](uint32_t y_i, uint32_t y_d) {
                      (void)y_i;
                      TRESULT->head->get_block(a_d)
                          ->get_block(b_d)
                          ->get_block(c_d)
                          ->get_block(x_d)
                          ->get_block(y_d)
                          ->set.foreach_index([&](uint32_t z_i, uint32_t z_d) {
                            (void)z_i;
                            std::cout
                                << ((Encoding<uint64_t> *)encodings_RESULT->at(
                                        0))->key_to_value[a_d] << "\t"
                                << ((Encoding<uint64_t> *)encodings_RESULT->at(
                                        1))->key_to_value[b_d] << "\t"
                                << ((Encoding<uint64_t> *)encodings_RESULT->at(
                                        2))->key_to_value[c_d] << "\t"
                                << ((Encoding<uint64_t> *)encodings_RESULT->at(
                                        3))->key_to_value[x_d] << "\t"
                                << ((Encoding<uint64_t> *)encodings_RESULT->at(
                                        4))->key_to_value[y_d] << "\t"
                                << ((Encoding<uint64_t> *)encodings_RESULT->at(
                                        5))->key_to_value[z_d] << std::endl;
                          });
                    });
              });
        });
      });
    });
  }
}
int main() {
  std::unordered_map<std::string, void *> relations;
  std::unordered_map<std::string, Trie<hybrid> *> tries;
  std::unordered_map<std::string, std::vector<void *> *> encodings;
  run(relations, tries, encodings);
}
