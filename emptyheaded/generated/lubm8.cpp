#include "emptyheaded.hpp"
extern "C" void
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
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
    Relation<std::string, std::string> *emailAddress =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/emailAddress.txt");
    char *next = f_reader.tsv_get_first();
    emailAddress->num_rows = 0;
    while (next != NULL) {
      emailAddress->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      emailAddress->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      emailAddress->num_rows++;
    }
    relations["emailAddress"] = emailAddress;
    std::cout << emailAddress->num_rows << " rows loaded." << std::endl;
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
    Relation<std::string, std::string> *emailAddress =
        (Relation<std::string, std::string> *)relations["emailAddress"];
    Relation<std::string, std::string> *memberOf =
        (Relation<std::string, std::string> *)relations["memberOf"];
    Relation<std::string, std::string> *subOrganizationOf =
        (Relation<std::string, std::string> *)relations["subOrganizationOf"];
    Relation<std::string, std::string> *type =
        (Relation<std::string, std::string> *)relations["type"];
    std::vector<Column<std::string>> *c_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *ab_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *e_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *df_attributes =
        new std::vector<Column<std::string>>();
    c_attributes->push_back(emailAddress->get<1>());
    e_attributes->push_back(subOrganizationOf->get<0>());
    df_attributes->push_back(type->get<1>());
    ab_attributes->push_back(memberOf->get<0>());
    ab_attributes->push_back(memberOf->get<1>());
    ab_attributes->push_back(emailAddress->get<0>());
    ab_attributes->push_back(type->get<0>());
    ab_attributes->push_back(subOrganizationOf->get<1>());
    Encoding<std::string> *c_encoding = new Encoding<std::string>(c_attributes);
    Encoding<std::string> *ab_encoding =
        new Encoding<std::string>(ab_attributes);
    Encoding<std::string> *e_encoding = new Encoding<std::string>(e_attributes);
    Encoding<std::string> *df_encoding =
        new Encoding<std::string>(df_attributes);
    std::vector<Column<uint32_t>> *EemailAddress =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_emailAddress = new std::vector<void *>();
    EemailAddress->push_back(ab_encoding->encoded.at(2));
    encodings_emailAddress->push_back((void *)ab_encoding);
    EemailAddress->push_back(c_encoding->encoded.at(0));
    encodings_emailAddress->push_back((void *)c_encoding);
    std::vector<Column<uint32_t>> *EmemberOf =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_memberOf = new std::vector<void *>();
    EmemberOf->push_back(ab_encoding->encoded.at(0));
    encodings_memberOf->push_back((void *)ab_encoding);
    EmemberOf->push_back(ab_encoding->encoded.at(1));
    encodings_memberOf->push_back((void *)ab_encoding);
    std::vector<Column<uint32_t>> *EsubOrganizationOf =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_subOrganizationOf =
        new std::vector<void *>();
    EsubOrganizationOf->push_back(ab_encoding->encoded.at(4));
    encodings_subOrganizationOf->push_back((void *)ab_encoding);
    EsubOrganizationOf->push_back(e_encoding->encoded.at(0));
    encodings_subOrganizationOf->push_back((void *)e_encoding);
    std::vector<Column<uint32_t>> *Etype = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_type = new std::vector<void *>();
    Etype->push_back(ab_encoding->encoded.at(3));
    encodings_type->push_back((void *)ab_encoding);
    Etype->push_back(df_encoding->encoded.at(0));
    encodings_type->push_back((void *)df_encoding);
    Trie<hybrid> *TemailAddress =
        Trie<hybrid>::build(EemailAddress, [&](size_t index) {
          (void)index;
          return true;
        });
    tries["emailAddress"] = TemailAddress;
    encodings["emailAddress"] = encodings_emailAddress;
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
    allocator::memory<uint8_t> output_buffer(
        5 * 6 * 2 * sizeof(TrieBlock<hybrid>) *
        (c_encoding->num_distinct + ab_encoding->num_distinct +
         e_encoding->num_distinct + df_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        5 * 6 * 2 * sizeof(TrieBlock<hybrid>) *
        (c_encoding->num_distinct + ab_encoding->num_distinct +
         e_encoding->num_distinct + df_encoding->num_distinct));
    uint32_t b_selection =
        ab_encoding->value_to_key.at("http://www.Department12.University8.edu");
    uint32_t d_selection =
        df_encoding->value_to_key.at("http://www.lehigh.edu/~zhp2/2004/0401/"
                                     "univ-bench.owl#UndergraduateStudent");
    uint32_t f_selection = df_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> subOrganizationOf_be_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *subOrganizationOf_be_block;
    {
      Set<hybrid> b;
      subOrganizationOf_be_block = NULL;
      if (TsubOrganizationOf->head) {
        b = TsubOrganizationOf->head->set;
        subOrganizationOf_be_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(TsubOrganizationOf->head);
        uint8_t *sd_b = output_buffer.get_next(
            0, b.cardinality * sizeof(uint32_t)); // initialize the memory
        uint32_t *ob_b = (uint32_t *)output_buffer.get_next(
            0, b.cardinality * sizeof(uint32_t));
        size_t ob_i_b = 0;
        b.foreach ([&](uint32_t b_data) {
          if (b_data == b_selection)
            ob_b[ob_i_b++] = b_data;
        });
        b = Set<hybrid>::from_array(sd_b, ob_b, ob_i_b);
        output_buffer.roll_back(0, b.cardinality * sizeof(uint32_t));
        subOrganizationOf_be_block->set = &b;
        subOrganizationOf_be_block->init_pointers(
            0, &output_buffer, b.cardinality, ab_encoding->num_distinct,
            b.type == type::UINTEGER);
      }
      b.par_foreach_index([&](size_t tid, uint32_t b_i, uint32_t b_d) {
        Set<hybrid> e;
        TrieBlock<hybrid> *e_block = NULL;
        if (TsubOrganizationOf->head->get_block(b_d)) {
          e = TsubOrganizationOf->head->get_block(b_d)->set;
          e_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>(TsubOrganizationOf->head->get_block(b_d));
        }
        if (e.cardinality != 0) {
          const size_t count = e.cardinality;
          subOrganizationOf_be_cardinality.update(tid, count);
          subOrganizationOf_be_block->set_block(b_i, b_d, e_block);
        } else {
          subOrganizationOf_be_block->set_block(b_i, b_d, NULL);
        }
      });
    }
    std::cout << subOrganizationOf_be_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> type_bf_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_bf_block;
    {
      Set<hybrid> b;
      type_bf_block = NULL;
      if (Ttype->head) {
        b = Ttype->head->set;
        type_bf_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(Ttype->head);
        uint8_t *sd_b = output_buffer.get_next(
            0, b.cardinality * sizeof(uint32_t)); // initialize the memory
        uint32_t *ob_b = (uint32_t *)output_buffer.get_next(
            0, b.cardinality * sizeof(uint32_t));
        size_t ob_i_b = 0;
        b.foreach ([&](uint32_t b_data) {
          if (b_data == b_selection)
            ob_b[ob_i_b++] = b_data;
        });
        b = Set<hybrid>::from_array(sd_b, ob_b, ob_i_b);
        output_buffer.roll_back(0, b.cardinality * sizeof(uint32_t));
        type_bf_block->set = &b;
        type_bf_block->init_pointers(0, &output_buffer, b.cardinality,
                                     ab_encoding->num_distinct,
                                     b.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, b.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      b.par_foreach_index([&](size_t tid, uint32_t b_i, uint32_t b_d) {
        Set<hybrid> f;
        if (Ttype->head->get_block(b_d)) {
          f = Ttype->head->get_block(b_d)->set;
          uint8_t *sd_f = output_buffer.get_next(
              tid, f.cardinality * sizeof(uint32_t)); // initialize the memory
          uint32_t *ob_f = (uint32_t *)output_buffer.get_next(
              tid, f.cardinality * sizeof(uint32_t));
          size_t ob_i_f = 0;
          f.foreach ([&](uint32_t f_data) {
            if (f_data == f_selection)
              ob_f[ob_i_f++] = f_data;
          });
          f = Set<hybrid>::from_array(sd_f, ob_f, ob_i_f);
          output_buffer.roll_back(tid, f.cardinality * sizeof(uint32_t));
        }
        if (f.cardinality != 0) {
          const size_t count = 1;
          type_bf_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = b_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * ab_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      b = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      type_bf_block->set = &b;
      output_buffer.roll_back(0, halloc_size - b.number_of_bytes);
    }
    std::cout << type_bf_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    TrieBlock<hybrid> *emailAddress_ac_block = TemailAddress->head;
    //////////NPRR
    par::reducer<size_t> type_ad_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_ad_block;
    {
      Set<hybrid> a;
      type_ad_block = NULL;
      if (Ttype->head) {
        a = Ttype->head->set;
        type_ad_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(Ttype->head);
        type_ad_block->init_pointers(0, &output_buffer, a.cardinality,
                                     ab_encoding->num_distinct,
                                     a.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, a.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> d;
        if (Ttype->head->get_block(a_d)) {
          d = Ttype->head->get_block(a_d)->set;
          uint8_t *sd_d = output_buffer.get_next(
              tid, d.cardinality * sizeof(uint32_t)); // initialize the memory
          uint32_t *ob_d = (uint32_t *)output_buffer.get_next(
              tid, d.cardinality * sizeof(uint32_t));
          size_t ob_i_d = 0;
          d.foreach ([&](uint32_t d_data) {
            if (d_data == d_selection)
              ob_d[ob_i_d++] = d_data;
          });
          d = Set<hybrid>::from_array(sd_d, ob_d, ob_i_d);
          output_buffer.roll_back(tid, d.cardinality * sizeof(uint32_t));
        }
        if (d.cardinality != 0) {
          const size_t count = 1;
          type_ad_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = a_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * ab_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      a = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      type_ad_block->set = &a;
      output_buffer.roll_back(0, halloc_size - a.number_of_bytes);
    }
    std::cout << type_ad_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> memberOf_ab_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *memberOf_ab_block;
    {
      Set<hybrid> a;
      memberOf_ab_block = NULL;
      if (TmemberOf->head && emailAddress_ac_block && type_ad_block) {
        memberOf_ab_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size_a =
            sizeof(uint64_t) * ab_encoding->num_distinct * 2;
        a.data =
            output_buffer.get_next(0, alloc_size_a); // initialize the memory
        Set<hybrid> a_tmp(
            tmp_buffer.get_next(0, alloc_size_a)); // initialize the memory
        a_tmp = *ops::set_intersect(
                    &a_tmp, (const Set<hybrid> *)&TmemberOf->head->set,
                    (const Set<hybrid> *)&emailAddress_ac_block->set);
        a = *ops::set_intersect(&a, (const Set<hybrid> *)&a_tmp,
                                (const Set<hybrid> *)&type_ad_block->set);
        tmp_buffer.roll_back(0, alloc_size_a);
        output_buffer.roll_back(0, alloc_size_a - a.number_of_bytes);
        memberOf_ab_block->set = &a;
        memberOf_ab_block->init_pointers(0, &output_buffer, a.cardinality,
                                         ab_encoding->num_distinct,
                                         a.type == type::UINTEGER);
      }
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> b;
        TrieBlock<hybrid> *b_block = NULL;
        if (TmemberOf->head->get_block(a_d) && subOrganizationOf_be_block &&
            type_bf_block) {
          b_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>();
          const size_t alloc_size_b =
              sizeof(uint64_t) * ab_encoding->num_distinct * 2;
          b.data = output_buffer.get_next(
              tid, alloc_size_b); // initialize the memory
          Set<hybrid> b_tmp(
              tmp_buffer.get_next(tid, alloc_size_b)); // initialize the memory
          b_tmp =
              *ops::set_intersect(
                  &b_tmp,
                  (const Set<hybrid> *)&TmemberOf->head->get_block(a_d)->set,
                  (const Set<hybrid> *)&subOrganizationOf_be_block->set,
                  [&](uint32_t b_data, uint32_t _1, uint32_t _2) {
                    return b_data == b_selection;
                  });
          b = *ops::set_intersect(&b, (const Set<hybrid> *)&b_tmp,
                                  (const Set<hybrid> *)&type_bf_block->set);
          tmp_buffer.roll_back(tid, alloc_size_b);
          output_buffer.roll_back(tid, alloc_size_b - b.number_of_bytes);
          b_block->set = &b;
        }
        if (b.cardinality != 0) {
          const size_t count = b.cardinality;
          memberOf_ab_cardinality.update(tid, count);
          memberOf_ab_block->set_block(a_i, a_d, b_block);
        } else {
          memberOf_ab_block->set_block(a_i, a_d, NULL);
        }
      });
    }
    std::cout << memberOf_ab_cardinality.evaluate(0) << std::endl;
    ///////////////////TOP DOWN
    par::reducer<size_t> result_abce_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *result_abce_block;
    {
      result_abce_block = memberOf_ab_block;
      if (result_abce_block) {
        result_abce_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(result_abce_block);
        result_abce_block->init_pointers(
            0, &output_buffer, result_abce_block->set.cardinality,
            ab_encoding->num_distinct,
            result_abce_block->set.type == type::UINTEGER);
        result_abce_block->set.par_foreach_index([&](size_t tid, uint32_t a_i,
                                                     uint32_t a_d) {
          (void)a_i;
          (void)a_d;
          TrieBlock<hybrid> *b_block = memberOf_ab_block->get_block(a_d);
          TrieBlock<hybrid> *c_block = emailAddress_ac_block->get_block(a_d);
          if (b_block && c_block) {
            b_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>(b_block);
            result_abce_block->set_block(a_i, a_d, b_block);
            b_block->init_pointers(
                tid, &output_buffer, b_block->set.cardinality,
                ab_encoding->num_distinct, b_block->set.type == type::UINTEGER);
            b_block->set.foreach_index([&](uint32_t b_i, uint32_t b_d) {
              (void)b_i;
              (void)b_d;
              TrieBlock<hybrid> *e_block =
                  subOrganizationOf_be_block->get_block(b_d);
              if (c_block && e_block) {
                c_block =
                    new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(c_block);
                b_block->set_block(b_i, b_d, c_block);
                c_block->init_pointers(tid, &output_buffer,
                                       c_block->set.cardinality,
                                       c_encoding->num_distinct,
                                       c_block->set.type == type::UINTEGER);
                c_block->set.foreach_index([&](uint32_t c_i, uint32_t c_d) {
                  (void)c_i;
                  (void)c_d;
                  if (e_block) {
                    e_block = new (
                        output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(e_block);
                    c_block->set_block(c_i, c_d, e_block);
                    const size_t count = e_block->set.cardinality;
                    result_abce_cardinality.update(tid, count);
                  }
                });
              }
            });
          }
        });
      }
      Trie<hybrid> *Tresult = new Trie<hybrid>(result_abce_block);
      tries["result"] = Tresult;
      std::vector<void *> *encodings_result = new std::vector<void *>();
      encodings_result->push_back(ab_encoding);
      encodings_result->push_back(ab_encoding);
      encodings_result->push_back(c_encoding);
      encodings_result->push_back(e_encoding);
      encodings["result"] = encodings_result;
    }
    std::cout << result_abce_cardinality.evaluate(0) << std::endl;
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
