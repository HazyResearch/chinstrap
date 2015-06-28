#include "emptyheaded.hpp"
extern "C" void
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<std::string, std::string> *undegraduateDegreeFrom =
        new Relation<std::string, std::string>();
    tsv_reader f_reader("/dfs/scratch0/caberger/systems/lubm/out_data/"
                        "undergraduateDegreeFrom.txt");
    char *next = f_reader.tsv_get_first();
    undegraduateDegreeFrom->num_rows = 0;
    while (next != NULL) {
      undegraduateDegreeFrom->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      undegraduateDegreeFrom->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      undegraduateDegreeFrom->num_rows++;
    }
    relations["undegraduateDegreeFrom"] = undegraduateDegreeFrom;
    std::cout << undegraduateDegreeFrom->num_rows << " rows loaded."
              << std::endl;
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<std::string, std::string> *memberOf =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/memberOf.txt");
    char *next = f_reader.tsv_get_first();
    memberOf->num_rows = 0;
    while (next != NULL) {
      memberOf->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      memberOf->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      memberOf->num_rows++;
    }
    relations["memberOf"] = memberOf;
    std::cout << memberOf->num_rows << " rows loaded." << std::endl;
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<std::string, std::string> *subOrganizationOf =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/subOrganizationOf.txt");
    char *next = f_reader.tsv_get_first();
    subOrganizationOf->num_rows = 0;
    while (next != NULL) {
      subOrganizationOf->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      subOrganizationOf->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      subOrganizationOf->num_rows++;
    }
    relations["subOrganizationOf"] = subOrganizationOf;
    std::cout << subOrganizationOf->num_rows << " rows loaded." << std::endl;
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<std::string, std::string> *type =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/type.txt");
    char *next = f_reader.tsv_get_first();
    type->num_rows = 0;
    while (next != NULL) {
      type->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      type->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      type->num_rows++;
    }
    relations["type"] = type;
    std::cout << type->num_rows << " rows loaded." << std::endl;
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<std::string, std::string> *memberOf =
        (Relation<std::string, std::string> *)relations["memberOf"];
    Relation<std::string, std::string> *subOrganizationOf =
        (Relation<std::string, std::string> *)relations["subOrganizationOf"];
    Relation<std::string, std::string> *type =
        (Relation<std::string, std::string> *)relations["type"];
    Relation<std::string, std::string> *undegraduateDegreeFrom =
        (Relation<std::string, std::string> *)
            relations["undegraduateDegreeFrom"];
    std::vector<Column<std::string>> *abc_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *xyz_attributes =
        new std::vector<Column<std::string>>();
    abc_attributes->push_back(memberOf->get<0>());
    abc_attributes->push_back(memberOf->get<1>());
    abc_attributes->push_back(subOrganizationOf->get<0>());
    abc_attributes->push_back(subOrganizationOf->get<1>());
    abc_attributes->push_back(undegraduateDegreeFrom->get<0>());
    abc_attributes->push_back(undegraduateDegreeFrom->get<1>());
    abc_attributes->push_back(type->get<0>());
    xyz_attributes->push_back(type->get<1>());
    Encoding<std::string> *abc_encoding =
        new Encoding<std::string>(abc_attributes);
    Encoding<std::string> *xyz_encoding =
        new Encoding<std::string>(xyz_attributes);
    std::vector<Column<uint32_t>> *EmemberOf =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_memberOf = new std::vector<void *>();
    EmemberOf->push_back(abc_encoding->encoded.at(0));
    encodings_memberOf->push_back((void *)abc_encoding);
    EmemberOf->push_back(abc_encoding->encoded.at(1));
    encodings_memberOf->push_back((void *)abc_encoding);
    std::vector<Column<uint32_t>> *EsubOrganizationOf =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_subOrganizationOf =
        new std::vector<void *>();
    EsubOrganizationOf->push_back(abc_encoding->encoded.at(3));
    encodings_subOrganizationOf->push_back((void *)abc_encoding);
    EsubOrganizationOf->push_back(abc_encoding->encoded.at(2));
    encodings_subOrganizationOf->push_back((void *)abc_encoding);
    std::vector<Column<uint32_t>> *Etype = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_type = new std::vector<void *>();
    Etype->push_back(abc_encoding->encoded.at(6));
    encodings_type->push_back((void *)abc_encoding);
    Etype->push_back(xyz_encoding->encoded.at(0));
    encodings_type->push_back((void *)xyz_encoding);
    std::vector<Column<uint32_t>> *EundegraduateDegreeFrom =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_undegraduateDegreeFrom =
        new std::vector<void *>();
    EundegraduateDegreeFrom->push_back(abc_encoding->encoded.at(5));
    encodings_undegraduateDegreeFrom->push_back((void *)abc_encoding);
    EundegraduateDegreeFrom->push_back(abc_encoding->encoded.at(4));
    encodings_undegraduateDegreeFrom->push_back((void *)abc_encoding);
    Trie<hybrid> *TmemberOf = Trie<hybrid>::build(EmemberOf, [&](size_t index) {
      (void)index;
      return true;
    });
    tries["memberOf"] = TmemberOf;
    encodings["memberOf"] = encodings_memberOf;
    Trie<hybrid> *TsubOrganizationOf =
        Trie<hybrid>::build(EsubOrganizationOf, [&](size_t index) {
          (void)index;
          return true;
        });
    tries["subOrganizationOf"] = TsubOrganizationOf;
    encodings["subOrganizationOf"] = encodings_subOrganizationOf;
    Trie<hybrid> *Ttype = Trie<hybrid>::build(Etype, [&](size_t index) {
      (void)index;
      return true;
    });
    tries["type"] = Ttype;
    encodings["type"] = encodings_type;
    Trie<hybrid> *TundegraduateDegreeFrom =
        Trie<hybrid>::build(EundegraduateDegreeFrom, [&](size_t index) {
          (void)index;
          return true;
        });
    tries["undegraduateDegreeFrom"] = TundegraduateDegreeFrom;
    encodings["undegraduateDegreeFrom"] = encodings_undegraduateDegreeFrom;
    allocator::memory<uint8_t> output_buffer(
        4 * 6 * 2 * sizeof(TrieBlock<hybrid>) *
        (abc_encoding->num_distinct + xyz_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        4 * 6 * 2 * sizeof(TrieBlock<hybrid>) *
        (abc_encoding->num_distinct + xyz_encoding->num_distinct));
    uint32_t y_selection = xyz_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
    uint32_t x_selection = xyz_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
    uint32_t z_selection = xyz_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> type_by_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_by_block;
    {
      Set<hybrid> b;
      type_by_block = NULL;
      if (Ttype->head) {
        b = Ttype->head->set;
        type_by_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(Ttype->head);
        type_by_block->init_pointers(0, &output_buffer, b.cardinality,
                                     abc_encoding->num_distinct,
                                     b.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, b.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      b.par_foreach_index([&](size_t tid, uint32_t b_i, uint32_t b_d) {
        Set<hybrid> y;
        if (Ttype->head->get_block(b_d)) {
          y = Ttype->head->get_block(b_d)->set;
          uint8_t *sd_y = output_buffer.get_next(
              tid, y.cardinality * sizeof(uint32_t)); // initialize the memory
          uint32_t *ob_y = (uint32_t *)output_buffer.get_next(
              tid, y.cardinality * sizeof(uint32_t));
          size_t ob_i_y = 0;
          y.foreach ([&](uint32_t y_data) {
            if (y_data == y_selection)
              ob_y[ob_i_y++] = y_data;
          });
          y = Set<hybrid>::from_array(sd_y, ob_y, ob_i_y);
          output_buffer.roll_back(tid, y.cardinality * sizeof(uint32_t));
        }
        if (y.cardinality != 0) {
          const size_t count = 1;
          type_by_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = b_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * abc_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      b = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      type_by_block->set = &b;
      output_buffer.roll_back(0, halloc_size - b.number_of_bytes);
    }
    std::cout << type_by_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> type_ax_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_ax_block;
    {
      Set<hybrid> a;
      type_ax_block = NULL;
      if (Ttype->head) {
        a = Ttype->head->set;
        type_ax_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(Ttype->head);
        type_ax_block->init_pointers(0, &output_buffer, a.cardinality,
                                     abc_encoding->num_distinct,
                                     a.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, a.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> x;
        if (Ttype->head->get_block(a_d)) {
          x = Ttype->head->get_block(a_d)->set;
          uint8_t *sd_x = output_buffer.get_next(
              tid, x.cardinality * sizeof(uint32_t)); // initialize the memory
          uint32_t *ob_x = (uint32_t *)output_buffer.get_next(
              tid, x.cardinality * sizeof(uint32_t));
          size_t ob_i_x = 0;
          x.foreach ([&](uint32_t x_data) {
            if (x_data == x_selection)
              ob_x[ob_i_x++] = x_data;
          });
          x = Set<hybrid>::from_array(sd_x, ob_x, ob_i_x);
          output_buffer.roll_back(tid, x.cardinality * sizeof(uint32_t));
        }
        if (x.cardinality != 0) {
          const size_t count = 1;
          type_ax_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = a_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * abc_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      a = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      type_ax_block->set = &a;
      output_buffer.roll_back(0, halloc_size - a.number_of_bytes);
    }
    std::cout << type_ax_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> type_cz_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_cz_block;
    {
      Set<hybrid> c;
      type_cz_block = NULL;
      if (Ttype->head) {
        c = Ttype->head->set;
        type_cz_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(Ttype->head);
        type_cz_block->init_pointers(0, &output_buffer, c.cardinality,
                                     abc_encoding->num_distinct,
                                     c.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, c.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      c.par_foreach_index([&](size_t tid, uint32_t c_i, uint32_t c_d) {
        Set<hybrid> z;
        if (Ttype->head->get_block(c_d)) {
          z = Ttype->head->get_block(c_d)->set;
          uint8_t *sd_z = output_buffer.get_next(
              tid, z.cardinality * sizeof(uint32_t)); // initialize the memory
          uint32_t *ob_z = (uint32_t *)output_buffer.get_next(
              tid, z.cardinality * sizeof(uint32_t));
          size_t ob_i_z = 0;
          z.foreach ([&](uint32_t z_data) {
            if (z_data == z_selection)
              ob_z[ob_i_z++] = z_data;
          });
          z = Set<hybrid>::from_array(sd_z, ob_z, ob_i_z);
          output_buffer.roll_back(tid, z.cardinality * sizeof(uint32_t));
        }
        if (z.cardinality != 0) {
          const size_t count = 1;
          type_cz_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = c_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * abc_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      c = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      type_cz_block->set = &c;
      output_buffer.roll_back(0, halloc_size - c.number_of_bytes);
    }
    std::cout << type_cz_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t>
        memberOfsubOrganizationOfundegraduateDegreeFrom_cab_cardinality(
            0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *
        memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block;
    {
      Set<hybrid> c;
      memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block = NULL;
      if (TsubOrganizationOf->head && TundegraduateDegreeFrom->head &&
          type_cz_block) {
        memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size_c =
            sizeof(uint64_t) * abc_encoding->num_distinct * 2;
        c.data =
            output_buffer.get_next(0, alloc_size_c); // initialize the memory
        Set<hybrid> c_tmp(
            tmp_buffer.get_next(0, alloc_size_c)); // initialize the memory
        c_tmp = *ops::set_intersect(
                    &c_tmp, (const Set<hybrid> *)&TsubOrganizationOf->head->set,
                    (const Set<hybrid> *)&TundegraduateDegreeFrom->head->set);
        c = *ops::set_intersect(&c, (const Set<hybrid> *)&c_tmp,
                                (const Set<hybrid> *)&type_cz_block->set);
        tmp_buffer.roll_back(0, alloc_size_c);
        output_buffer.roll_back(0, alloc_size_c - c.number_of_bytes);
        memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block->set = &c;
        memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block
            ->init_pointers(0, &output_buffer, c.cardinality,
                            abc_encoding->num_distinct,
                            c.type == type::UINTEGER);
      }
      c.par_foreach_index([&](size_t tid, uint32_t c_i, uint32_t c_d) {
        Set<hybrid> a;
        TrieBlock<hybrid> *a_block = NULL;
        if (TmemberOf->head && TundegraduateDegreeFrom->head->get_block(c_d) &&
            type_ax_block) {
          a_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>();
          const size_t alloc_size_a =
              sizeof(uint64_t) * abc_encoding->num_distinct * 2;
          a.data = output_buffer.get_next(
              tid, alloc_size_a); // initialize the memory
          Set<hybrid> a_tmp(
              tmp_buffer.get_next(tid, alloc_size_a)); // initialize the memory
          a_tmp = *ops::set_intersect(
                      &a_tmp, (const Set<hybrid> *)&TmemberOf->head->set,
                      (const Set<hybrid> *)&TundegraduateDegreeFrom->head
                          ->get_block(c_d)
                          ->set);
          a = *ops::set_intersect(&a, (const Set<hybrid> *)&a_tmp,
                                  (const Set<hybrid> *)&type_ax_block->set);
          tmp_buffer.roll_back(tid, alloc_size_a);
          output_buffer.roll_back(tid, alloc_size_a - a.number_of_bytes);
          a_block->set = &a;
          a_block->init_pointers(tid, &output_buffer, a.cardinality,
                                 abc_encoding->num_distinct,
                                 a.type == type::UINTEGER);
        }
        bool a_block_valid = false;
        a.foreach_index([&](uint32_t a_i, uint32_t a_d) {
          Set<hybrid> b;
          TrieBlock<hybrid> *b_block = NULL;
          if (TmemberOf->head->get_block(a_d) &&
              TsubOrganizationOf->head->get_block(c_d) && type_by_block) {
            b_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>();
            const size_t alloc_size_b =
                sizeof(uint64_t) * abc_encoding->num_distinct * 2;
            b.data = output_buffer.get_next(
                tid, alloc_size_b); // initialize the memory
            Set<hybrid> b_tmp(tmp_buffer.get_next(
                tid, alloc_size_b)); // initialize the memory
            b_tmp =
                *ops::set_intersect(
                    &b_tmp,
                    (const Set<hybrid> *)&TmemberOf->head->get_block(a_d)->set,
                    (const Set<hybrid> *)&TsubOrganizationOf->head->get_block(
                                                                        c_d)
                        ->set);
            b = *ops::set_intersect(&b, (const Set<hybrid> *)&b_tmp,
                                    (const Set<hybrid> *)&type_by_block->set);
            tmp_buffer.roll_back(tid, alloc_size_b);
            output_buffer.roll_back(tid, alloc_size_b - b.number_of_bytes);
            b_block->set = &b;
          }
          if (b.cardinality != 0) {
            const size_t count = b.cardinality;
            memberOfsubOrganizationOfundegraduateDegreeFrom_cab_cardinality
                .update(tid, count);
            a_block_valid = true;
            a_block->set_block(a_i, a_d, b_block);
          } else {
            a_block->set_block(a_i, a_d, NULL);
          }
        });
        if (a_block_valid) {
          memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block->set_block(
              c_i, c_d, a_block);
        } else {
          memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block->set_block(
              c_i, c_d, NULL);
        }
      });
    }
    std::cout << memberOfsubOrganizationOfundegraduateDegreeFrom_cab_cardinality
                     .evaluate(0) << std::endl;
    ///////////////////TOP DOWN
    par::reducer<size_t> result_abc_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *result_abc_block;
    {
      result_abc_block =
          memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block;
      if (result_abc_block) {
        result_abc_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(result_abc_block);
        result_abc_block->init_pointers(
            0, &output_buffer, result_abc_block->set.cardinality,
            abc_encoding->num_distinct,
            result_abc_block->set.type == type::UINTEGER);
        result_abc_block->set.par_foreach_index([&](size_t tid, uint32_t c_i,
                                                    uint32_t c_d) {
          (void)c_i;
          (void)c_d;
          TrieBlock<hybrid> *a_block =
              memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block
                  ->get_block(c_d);
          if (a_block) {
            a_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>(a_block);
            result_abc_block->set_block(c_i, c_d, a_block);
            a_block->init_pointers(tid, &output_buffer,
                                   a_block->set.cardinality,
                                   abc_encoding->num_distinct,
                                   a_block->set.type == type::UINTEGER);
            a_block->set.foreach_index([&](uint32_t a_i, uint32_t a_d) {
              (void)a_i;
              (void)a_d;
              TrieBlock<hybrid> *b_block =
                  memberOfsubOrganizationOfundegraduateDegreeFrom_cab_block
                      ->get_block(c_d)
                      ->get_block(a_d);
              if (b_block) {
                b_block =
                    new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(b_block);
                a_block->set_block(a_i, a_d, b_block);
                const size_t count = b_block->set.cardinality;
                result_abc_cardinality.update(tid, count);
              }
            });
          }
        });
      }
      Trie<hybrid> *Tresult = new Trie<hybrid>(result_abc_block);
      tries["result"] = Tresult;
      std::vector<void *> *encodings_result = new std::vector<void *>();
      encodings_result->push_back(abc_encoding);
      encodings_result->push_back(abc_encoding);
      encodings_result->push_back(abc_encoding);
      encodings["result"] = encodings_result;
    }
    std::cout << result_abc_cardinality.evaluate(0) << std::endl;
    debug::stop_clock("JOIN", join_timer);
    tmp_buffer.free();
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Trie<hybrid> *Tresult = tries["result"];
    std::vector<void *> *encodings_result = encodings["result"];
    if (Tresult->head) {
      Tresult->head->set.foreach_index([&](uint32_t a_i, uint32_t a_d) {
        (void)a_i;
        if (Tresult->head->get_block(a_d)) {
          Tresult->head->get_block(a_d)->set.foreach_index([&](uint32_t b_i,
                                                               uint32_t b_d) {
            (void)b_i;
            if (Tresult->head->get_block(a_d)->get_block(b_d)) {
              Tresult->head->get_block(a_d)->get_block(b_d)->set.foreach_index(
                  [&](uint32_t c_i, uint32_t c_d) {
                    (void)c_i;
                    std::cout
                        << ((Encoding<std::string> *)encodings_result->at(0))
                               ->key_to_value[a_d] << "\t"
                        << ((Encoding<std::string> *)encodings_result->at(1))
                               ->key_to_value[b_d] << "\t"
                        << ((Encoding<std::string> *)encodings_result->at(2))
                               ->key_to_value[c_d] << std::endl;
                  });
            };
          });
        };
      });
    };
  }
}
int main() {
  std::unordered_map<std::string, void *> relations;
  std::unordered_map<std::string, Trie<hybrid> *> tries;
  std::unordered_map<std::string, std::vector<void *> *> encodings;
  run(relations, tries, encodings);
}
