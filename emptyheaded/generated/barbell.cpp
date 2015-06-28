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
    allocator::memory<uint8_t> output_buffer(3 * 6 * 2 *
                                             sizeof(TrieBlock<hybrid>) *
                                             (abcxyz_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(3 * 6 * 2 *
                                          sizeof(TrieBlock<hybrid>) *
                                          (abcxyz_encoding->num_distinct));
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> R_xyz_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *R_xyz_block;
    {
      Set<hybrid> x;
      R_xyz_block = NULL;
      if (TR->head) {
        x = TR->head->set;
        R_xyz_block = new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
            TrieBlock<hybrid>(TR->head);
        R_xyz_block->init_pointers(0, &output_buffer, x.cardinality,
                                   abcxyz_encoding->num_distinct,
                                   x.type == type::UINTEGER);
      }
      x.par_foreach_index([&](size_t tid, uint32_t x_i, uint32_t x_d) {
        Set<hybrid> y;
        TrieBlock<hybrid> *y_block = NULL;
        if (TR->head->get_block(x_d) && TR->head) {
          y_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
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
            z_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>();
            const size_t alloc_size_z =
                sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
            z.data = output_buffer.get_next(
                tid, alloc_size_z); // initialize the memory
            z = *ops::set_intersect(
                    &z, (const Set<hybrid> *)&TR->head->get_block(y_d)->set,
                    (const Set<hybrid> *)&TR->head->get_block(x_d)->set);
            output_buffer.roll_back(tid, alloc_size_z - z.number_of_bytes);
            z_block->set = &z;
          }
          if (z.cardinality != 0) {
            const size_t count = z.cardinality;
            R_xyz_cardinality.update(tid, count);
            y_block_valid = true;
            y_block->set_block(y_i, y_d, z_block);
          } else {
            y_block->set_block(y_i, y_d, NULL);
          }
        });
        if (y_block_valid) {
          R_xyz_block->set_block(x_i, x_d, y_block);
        } else {
          R_xyz_block->set_block(x_i, x_d, NULL);
        }
      });
    }
    std::cout << R_xyz_cardinality.evaluate(0) << std::endl;
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
    //////////NPRR
    par::reducer<size_t> R_ax_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *R_ax_block;
    {
      Set<hybrid> a;
      R_ax_block = NULL;
      if (TR->head && R_abc_block) {
        R_ax_block = new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
            TrieBlock<hybrid>();
        const size_t alloc_size_a =
            sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
        a.data =
            output_buffer.get_next(0, alloc_size_a); // initialize the memory
        a = *ops::set_intersect(&a, (const Set<hybrid> *)&TR->head->set,
                                (const Set<hybrid> *)&R_abc_block->set);
        output_buffer.roll_back(0, alloc_size_a - a.number_of_bytes);
        R_ax_block->set = &a;
        R_ax_block->init_pointers(0, &output_buffer, a.cardinality,
                                  abcxyz_encoding->num_distinct,
                                  a.type == type::UINTEGER);
      }
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> x;
        TrieBlock<hybrid> *x_block = NULL;
        if (TR->head->get_block(a_d) && R_xyz_block) {
          x_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>();
          const size_t alloc_size_x =
              sizeof(uint64_t) * abcxyz_encoding->num_distinct * 2;
          x.data = output_buffer.get_next(
              tid, alloc_size_x); // initialize the memory
          x = *ops::set_intersect(
                  &x, (const Set<hybrid> *)&TR->head->get_block(a_d)->set,
                  (const Set<hybrid> *)&R_xyz_block->set);
          output_buffer.roll_back(tid, alloc_size_x - x.number_of_bytes);
          x_block->set = &x;
        }
        if (x.cardinality != 0) {
          const size_t count = x.cardinality;
          R_ax_cardinality.update(tid, count);
          R_ax_block->set_block(a_i, a_d, x_block);
        } else {
          R_ax_block->set_block(a_i, a_d, NULL);
        }
      });
    }
    std::cout << R_ax_cardinality.evaluate(0) << std::endl;
    ///////////////////TOP DOWN
    par::reducer<size_t> RESULT_axbcyz_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *RESULT_axbcyz_block;
    {
      RESULT_axbcyz_block = R_ax_block;
      if (RESULT_axbcyz_block) {
        RESULT_axbcyz_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(RESULT_axbcyz_block);
        RESULT_axbcyz_block->init_pointers(
            0, &output_buffer, RESULT_axbcyz_block->set.cardinality,
            abcxyz_encoding->num_distinct,
            RESULT_axbcyz_block->set.type == type::UINTEGER);
        RESULT_axbcyz_block->set.par_foreach_index([&](size_t tid, uint32_t a_i,
                                                       uint32_t a_d) {
          (void)a_i;
          (void)a_d;
          TrieBlock<hybrid> *x_block = R_ax_block->get_block(a_d);
          TrieBlock<hybrid> *b_block = R_abc_block->get_block(a_d);
          if (x_block && b_block) {
            x_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>(x_block);
            RESULT_axbcyz_block->set_block(a_i, a_d, x_block);
            x_block->init_pointers(tid, &output_buffer,
                                   x_block->set.cardinality,
                                   abcxyz_encoding->num_distinct,
                                   x_block->set.type == type::UINTEGER);
            x_block->set.foreach_index([&](uint32_t x_i, uint32_t x_d) {
              (void)x_i;
              (void)x_d;
              TrieBlock<hybrid> *y_block = R_xyz_block->get_block(x_d);
              if (b_block && y_block) {
                b_block =
                    new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(b_block);
                x_block->set_block(x_i, x_d, b_block);
                b_block->init_pointers(tid, &output_buffer,
                                       b_block->set.cardinality,
                                       abcxyz_encoding->num_distinct,
                                       b_block->set.type == type::UINTEGER);
                b_block->set.foreach_index([&](uint32_t b_i, uint32_t b_d) {
                  (void)b_i;
                  (void)b_d;
                  TrieBlock<hybrid> *c_block =
                      R_abc_block->get_block(a_d)->get_block(b_d);
                  if (c_block) {
                    c_block = new (
                        output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(c_block);
                    b_block->set_block(b_i, b_d, c_block);
                    c_block->init_pointers(tid, &output_buffer,
                                           c_block->set.cardinality,
                                           abcxyz_encoding->num_distinct,
                                           c_block->set.type == type::UINTEGER);
                    c_block->set.foreach_index([&](uint32_t c_i, uint32_t c_d) {
                      (void)c_i;
                      (void)c_d;
                      if (y_block) {
                        y_block = new (output_buffer.get_next(
                            tid, sizeof(TrieBlock<hybrid>)))
                            TrieBlock<hybrid>(y_block);
                        c_block->set_block(c_i, c_d, y_block);
                        y_block->init_pointers(
                            tid, &output_buffer, y_block->set.cardinality,
                            abcxyz_encoding->num_distinct,
                            y_block->set.type == type::UINTEGER);
                        y_block->set.foreach_index(
                            [&](uint32_t y_i, uint32_t y_d) {
                              (void)y_i;
                              (void)y_d;
                              TrieBlock<hybrid> *z_block =
                                  R_xyz_block->get_block(x_d)->get_block(y_d);
                              if (z_block) {
                                z_block = new (output_buffer.get_next(
                                    tid, sizeof(TrieBlock<hybrid>)))
                                    TrieBlock<hybrid>(z_block);
                                y_block->set_block(y_i, y_d, z_block);
                                const size_t count = z_block->set.cardinality;
                                RESULT_axbcyz_cardinality.update(tid, count);
                              } else {
                                y_block->set_block(y_i, y_d, NULL);
                              }
                            });
                      } else {
                        c_block->set_block(c_i, c_d, NULL);
                      }
                    });
                  } else {
                    b_block->set_block(b_i, b_d, NULL);
                  }
                });
              } else {
                x_block->set_block(x_i, x_d, NULL);
              }
            });
          } else {
            RESULT_axbcyz_block->set_block(a_i, a_d, NULL);
          }
        });
      }
      Trie<hybrid> *TRESULT = new Trie<hybrid>(RESULT_axbcyz_block);
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
    std::cout << RESULT_axbcyz_cardinality.evaluate(0) << std::endl;
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
