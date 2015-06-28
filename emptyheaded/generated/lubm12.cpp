#include "emptyheaded.hpp"
extern "C" void
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<std::string, std::string> *worksFor =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/worksFor.txt");
    char *next = f_reader.tsv_get_first();
    worksFor->num_rows = 0;
    while (next != NULL) {
      worksFor->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      worksFor->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      worksFor->num_rows++;
    }
    relations["worksFor"] = worksFor;
    std::cout << worksFor->num_rows << " rows loaded." << std::endl;
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
    Relation<std::string, std::string> *subOrganizationOf =
        (Relation<std::string, std::string> *)relations["subOrganizationOf"];
    Relation<std::string, std::string> *type =
        (Relation<std::string, std::string> *)relations["type"];
    Relation<std::string, std::string> *worksFor =
        (Relation<std::string, std::string> *)relations["worksFor"];
    std::vector<Column<std::string>> *d_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *ab_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *ce_attributes =
        new std::vector<Column<std::string>>();
    d_attributes->push_back(subOrganizationOf->get<0>());
    ab_attributes->push_back(worksFor->get<0>());
    ab_attributes->push_back(worksFor->get<1>());
    ab_attributes->push_back(type->get<0>());
    ab_attributes->push_back(subOrganizationOf->get<1>());
    ce_attributes->push_back(type->get<1>());
    Encoding<std::string> *d_encoding = new Encoding<std::string>(d_attributes);
    Encoding<std::string> *ab_encoding =
        new Encoding<std::string>(ab_attributes);
    Encoding<std::string> *ce_encoding =
        new Encoding<std::string>(ce_attributes);
    std::vector<Column<uint32_t>> *EsubOrganizationOf =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_subOrganizationOf =
        new std::vector<void *>();
    EsubOrganizationOf->push_back(ab_encoding->encoded.at(3));
    encodings_subOrganizationOf->push_back((void *)ab_encoding);
    EsubOrganizationOf->push_back(d_encoding->encoded.at(0));
    encodings_subOrganizationOf->push_back((void *)d_encoding);
    std::vector<Column<uint32_t>> *Etype = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_type = new std::vector<void *>();
    Etype->push_back(ab_encoding->encoded.at(2));
    encodings_type->push_back((void *)ab_encoding);
    Etype->push_back(ce_encoding->encoded.at(0));
    encodings_type->push_back((void *)ce_encoding);
    std::vector<Column<uint32_t>> *EworksFor =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_worksFor = new std::vector<void *>();
    EworksFor->push_back(ab_encoding->encoded.at(1));
    encodings_worksFor->push_back((void *)ab_encoding);
    EworksFor->push_back(ab_encoding->encoded.at(0));
    encodings_worksFor->push_back((void *)ab_encoding);
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
    Trie<hybrid> *TworksFor = Trie<hybrid>::build(EworksFor, [&](size_t index) {
      (void)index;
      return true;
    });
    tries["worksFor"] = TworksFor;
    encodings["worksFor"] = encodings_worksFor;
    allocator::memory<uint8_t> output_buffer(
        4 * 5 * 2 * sizeof(TrieBlock<hybrid>) *
        (d_encoding->num_distinct + ab_encoding->num_distinct +
         ce_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        4 * 5 * 2 * sizeof(TrieBlock<hybrid>) *
        (d_encoding->num_distinct + ab_encoding->num_distinct +
         ce_encoding->num_distinct));
    uint32_t b_selection =
        ab_encoding->value_to_key.at("http://www.Department12.University8.edu");
    uint32_t c_selection = ce_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#FullProfessor");
    uint32_t e_selection = ce_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> type_ac_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_ac_block;
    {
      Set<hybrid> a;
      type_ac_block = NULL;
      if (Ttype->head) {
        a = Ttype->head->set;
        type_ac_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(Ttype->head);
        type_ac_block->init_pointers(0, &output_buffer, a.cardinality,
                                     ab_encoding->num_distinct,
                                     a.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, a.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> c;
        if (Ttype->head->get_block(a_d)) {
          c = Ttype->head->get_block(a_d)->set;
          uint8_t *sd_c = output_buffer.get_next(
              tid, c.cardinality * sizeof(uint32_t)); // initialize the memory
          uint32_t *ob_c = (uint32_t *)output_buffer.get_next(
              tid, c.cardinality * sizeof(uint32_t));
          size_t ob_i_c = 0;
          c.foreach ([&](uint32_t c_data) {
            if (c_data == c_selection)
              ob_c[ob_i_c++] = c_data;
          });
          c = Set<hybrid>::from_array(sd_c, ob_c, ob_i_c);
          output_buffer.roll_back(tid, c.cardinality * sizeof(uint32_t));
        }
        if (c.cardinality != 0) {
          const size_t count = 1;
          type_ac_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = a_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * ab_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      a = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      type_ac_block->set = &a;
      output_buffer.roll_back(0, halloc_size - a.number_of_bytes);
    }
    std::cout << type_ac_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> subOrganizationOf_bd_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *subOrganizationOf_bd_block;
    {
      Set<hybrid> b;
      subOrganizationOf_bd_block = NULL;
      if (TsubOrganizationOf->head) {
        b = TsubOrganizationOf->head->set;
        subOrganizationOf_bd_block =
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
        subOrganizationOf_bd_block->set = &b;
        subOrganizationOf_bd_block->init_pointers(
            0, &output_buffer, b.cardinality, ab_encoding->num_distinct,
            b.type == type::UINTEGER);
      }
      b.par_foreach_index([&](size_t tid, uint32_t b_i, uint32_t b_d) {
        Set<hybrid> d;
        TrieBlock<hybrid> *d_block = NULL;
        if (TsubOrganizationOf->head->get_block(b_d)) {
          d = TsubOrganizationOf->head->get_block(b_d)->set;
          d_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>(TsubOrganizationOf->head->get_block(b_d));
        }
        if (d.cardinality != 0) {
          const size_t count = d.cardinality;
          subOrganizationOf_bd_cardinality.update(tid, count);
          subOrganizationOf_bd_block->set_block(b_i, b_d, d_block);
        } else {
          subOrganizationOf_bd_block->set_block(b_i, b_d, NULL);
        }
      });
    }
    std::cout << subOrganizationOf_bd_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> type_be_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_be_block;
    {
      Set<hybrid> b;
      type_be_block = NULL;
      if (Ttype->head) {
        b = Ttype->head->set;
        type_be_block =
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
        type_be_block->set = &b;
        type_be_block->init_pointers(0, &output_buffer, b.cardinality,
                                     ab_encoding->num_distinct,
                                     b.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, b.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      b.par_foreach_index([&](size_t tid, uint32_t b_i, uint32_t b_d) {
        Set<hybrid> e;
        if (Ttype->head->get_block(b_d)) {
          e = Ttype->head->get_block(b_d)->set;
          uint8_t *sd_e = output_buffer.get_next(
              tid, e.cardinality * sizeof(uint32_t)); // initialize the memory
          uint32_t *ob_e = (uint32_t *)output_buffer.get_next(
              tid, e.cardinality * sizeof(uint32_t));
          size_t ob_i_e = 0;
          e.foreach ([&](uint32_t e_data) {
            if (e_data == e_selection)
              ob_e[ob_i_e++] = e_data;
          });
          e = Set<hybrid>::from_array(sd_e, ob_e, ob_i_e);
          output_buffer.roll_back(tid, e.cardinality * sizeof(uint32_t));
        }
        if (e.cardinality != 0) {
          const size_t count = 1;
          type_be_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = b_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * ab_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      b = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      type_be_block->set = &b;
      output_buffer.roll_back(0, halloc_size - b.number_of_bytes);
    }
    std::cout << type_be_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    par::reducer<size_t> worksFor_ba_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *worksFor_ba_block;
    {
      Set<hybrid> b;
      worksFor_ba_block = NULL;
      if (TworksFor->head && subOrganizationOf_bd_block && type_be_block) {
        worksFor_ba_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size_b =
            sizeof(uint64_t) * ab_encoding->num_distinct * 2;
        b.data =
            output_buffer.get_next(0, alloc_size_b); // initialize the memory
        Set<hybrid> b_tmp(
            tmp_buffer.get_next(0, alloc_size_b)); // initialize the memory
        b_tmp = *ops::set_intersect(
                    &b_tmp, (const Set<hybrid> *)&TworksFor->head->set,
                    (const Set<hybrid> *)&subOrganizationOf_bd_block->set,
                    [&](uint32_t b_data, uint32_t _1, uint32_t _2) {
                      return b_data == b_selection;
                    });
        b = *ops::set_intersect(&b, (const Set<hybrid> *)&b_tmp,
                                (const Set<hybrid> *)&type_be_block->set);
        tmp_buffer.roll_back(0, alloc_size_b);
        output_buffer.roll_back(0, alloc_size_b - b.number_of_bytes);
        worksFor_ba_block->set = &b;
        worksFor_ba_block->init_pointers(0, &output_buffer, b.cardinality,
                                         ab_encoding->num_distinct,
                                         b.type == type::UINTEGER);
      }
      b.par_foreach_index([&](size_t tid, uint32_t b_i, uint32_t b_d) {
        Set<hybrid> a;
        TrieBlock<hybrid> *a_block = NULL;
        if (TworksFor->head->get_block(b_d) && type_ac_block) {
          a_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>();
          const size_t alloc_size_a =
              sizeof(uint64_t) * ab_encoding->num_distinct * 2;
          a.data = output_buffer.get_next(
              tid, alloc_size_a); // initialize the memory
          a = *ops::set_intersect(
                  &a,
                  (const Set<hybrid> *)&TworksFor->head->get_block(b_d)->set,
                  (const Set<hybrid> *)&type_ac_block->set);
          output_buffer.roll_back(tid, alloc_size_a - a.number_of_bytes);
          a_block->set = &a;
        }
        if (a.cardinality != 0) {
          const size_t count = a.cardinality;
          worksFor_ba_cardinality.update(tid, count);
          worksFor_ba_block->set_block(b_i, b_d, a_block);
        } else {
          worksFor_ba_block->set_block(b_i, b_d, NULL);
        }
      });
    }
    std::cout << worksFor_ba_cardinality.evaluate(0) << std::endl;
    ///////////////////TOP DOWN
    par::reducer<size_t> result_abd_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *result_abd_block;
    {
      result_abd_block = worksFor_ba_block;
      if (result_abd_block) {
        result_abd_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(result_abd_block);
        result_abd_block->init_pointers(
            0, &output_buffer, result_abd_block->set.cardinality,
            ab_encoding->num_distinct,
            result_abd_block->set.type == type::UINTEGER);
        result_abd_block->set.par_foreach_index([&](size_t tid, uint32_t b_i,
                                                    uint32_t b_d) {
          (void)b_i;
          (void)b_d;
          TrieBlock<hybrid> *a_block = worksFor_ba_block->get_block(b_d);
          TrieBlock<hybrid> *d_block =
              subOrganizationOf_bd_block->get_block(b_d);
          if (a_block && d_block) {
            a_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>(a_block);
            result_abd_block->set_block(b_i, b_d, a_block);
            a_block->init_pointers(
                tid, &output_buffer, a_block->set.cardinality,
                ab_encoding->num_distinct, a_block->set.type == type::UINTEGER);
            a_block->set.foreach_index([&](uint32_t a_i, uint32_t a_d) {
              (void)a_i;
              (void)a_d;
              if (d_block) {
                d_block =
                    new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(d_block);
                a_block->set_block(a_i, a_d, d_block);
                const size_t count = d_block->set.cardinality;
                result_abd_cardinality.update(tid, count);
              }
            });
          }
        });
      }
      Trie<hybrid> *Tresult = new Trie<hybrid>(result_abd_block);
      tries["result"] = Tresult;
      std::vector<void *> *encodings_result = new std::vector<void *>();
      encodings_result->push_back(ab_encoding);
      encodings_result->push_back(ab_encoding);
      encodings_result->push_back(d_encoding);
      encodings["result"] = encodings_result;
    }
    std::cout << result_abd_cardinality.evaluate(0) << std::endl;
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
