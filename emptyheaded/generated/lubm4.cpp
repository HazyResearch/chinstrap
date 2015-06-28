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
    Relation<std::string, std::string> *name =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/name.txt");
    char *next = f_reader.tsv_get_first();
    name->num_rows = 0;
    while (next != NULL) {
      name->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      name->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      name->num_rows++;
    }
    relations["name"] = name;
    std::cout << name->num_rows << " rows loaded." << std::endl;
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
    Relation<std::string, std::string> *telephone =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/telephone.txt");
    char *next = f_reader.tsv_get_first();
    telephone->num_rows = 0;
    while (next != NULL) {
      telephone->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      telephone->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      telephone->num_rows++;
    }
    relations["telephone"] = telephone;
    std::cout << telephone->num_rows << " rows loaded." << std::endl;
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
    Relation<std::string, std::string> *emailAddress =
        (Relation<std::string, std::string> *)relations["emailAddress"];
    Relation<std::string, std::string> *name =
        (Relation<std::string, std::string> *)relations["name"];
    Relation<std::string, std::string> *telephone =
        (Relation<std::string, std::string> *)relations["telephone"];
    Relation<std::string, std::string> *type =
        (Relation<std::string, std::string> *)relations["type"];
    Relation<std::string, std::string> *worksFor =
        (Relation<std::string, std::string> *)relations["worksFor"];
    std::vector<Column<std::string>> *d_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *a_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *f_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *c_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *e_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *b_attributes =
        new std::vector<Column<std::string>>();
    d_attributes->push_back(emailAddress->get<1>());
    a_attributes->push_back(worksFor->get<0>());
    a_attributes->push_back(name->get<0>());
    a_attributes->push_back(emailAddress->get<0>());
    a_attributes->push_back(telephone->get<0>());
    a_attributes->push_back(type->get<0>());
    f_attributes->push_back(type->get<1>());
    c_attributes->push_back(name->get<1>());
    e_attributes->push_back(telephone->get<1>());
    b_attributes->push_back(worksFor->get<1>());
    Encoding<std::string> *d_encoding = new Encoding<std::string>(d_attributes);
    Encoding<std::string> *a_encoding = new Encoding<std::string>(a_attributes);
    Encoding<std::string> *f_encoding = new Encoding<std::string>(f_attributes);
    Encoding<std::string> *c_encoding = new Encoding<std::string>(c_attributes);
    Encoding<std::string> *e_encoding = new Encoding<std::string>(e_attributes);
    Encoding<std::string> *b_encoding = new Encoding<std::string>(b_attributes);
    std::vector<Column<uint32_t>> *EemailAddress =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_emailAddress = new std::vector<void *>();
    EemailAddress->push_back(a_encoding->encoded.at(2));
    encodings_emailAddress->push_back((void *)a_encoding);
    EemailAddress->push_back(d_encoding->encoded.at(0));
    encodings_emailAddress->push_back((void *)d_encoding);
    std::vector<Column<uint32_t>> *Ename = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_name = new std::vector<void *>();
    Ename->push_back(a_encoding->encoded.at(1));
    encodings_name->push_back((void *)a_encoding);
    Ename->push_back(c_encoding->encoded.at(0));
    encodings_name->push_back((void *)c_encoding);
    std::vector<Column<uint32_t>> *Etelephone =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_telephone = new std::vector<void *>();
    Etelephone->push_back(a_encoding->encoded.at(3));
    encodings_telephone->push_back((void *)a_encoding);
    Etelephone->push_back(e_encoding->encoded.at(0));
    encodings_telephone->push_back((void *)e_encoding);
    std::vector<Column<uint32_t>> *Etype = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_type = new std::vector<void *>();
    Etype->push_back(a_encoding->encoded.at(4));
    encodings_type->push_back((void *)a_encoding);
    Etype->push_back(f_encoding->encoded.at(0));
    encodings_type->push_back((void *)f_encoding);
    std::vector<Column<uint32_t>> *EworksFor =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_worksFor = new std::vector<void *>();
    EworksFor->push_back(a_encoding->encoded.at(0));
    encodings_worksFor->push_back((void *)a_encoding);
    EworksFor->push_back(b_encoding->encoded.at(0));
    encodings_worksFor->push_back((void *)b_encoding);
    Trie<hybrid> *TemailAddress =
        Trie<hybrid>::build(EemailAddress, [&](size_t index) {
          (void)index;
          return true;
        });
    tries["emailAddress"] = TemailAddress;
    encodings["emailAddress"] = encodings_emailAddress;
    Trie<hybrid> *Tname = Trie<hybrid>::build(Ename, [&](size_t index) {
      (void)index;
      return true;
    });
    tries["name"] = Tname;
    encodings["name"] = encodings_name;
    Trie<hybrid> *Ttelephone =
        Trie<hybrid>::build(Etelephone, [&](size_t index) {
          (void)index;
          return true;
        });
    tries["telephone"] = Ttelephone;
    encodings["telephone"] = encodings_telephone;
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
        5 * 6 * 2 * sizeof(TrieBlock<hybrid>) *
        (d_encoding->num_distinct + a_encoding->num_distinct +
         f_encoding->num_distinct + c_encoding->num_distinct +
         e_encoding->num_distinct + b_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        5 * 6 * 2 * sizeof(TrieBlock<hybrid>) *
        (d_encoding->num_distinct + a_encoding->num_distinct +
         f_encoding->num_distinct + c_encoding->num_distinct +
         e_encoding->num_distinct + b_encoding->num_distinct));
    uint32_t b_selection =
        b_encoding->value_to_key.at("http://www.Department12.University8.edu");
    uint32_t f_selection =
        f_encoding->value_to_key.at("http://www.lehigh.edu/~zhp2/2004/0401/"
                                    "univ-bench.owl#AssociateProfessor");
    auto join_timer = debug::start_clock();
    //////////NPRR
    TrieBlock<hybrid> *emailAddress_ad_block = TemailAddress->head;
    //////////NPRR
    TrieBlock<hybrid> *telephone_ae_block = Ttelephone->head;
    //////////NPRR
    par::reducer<size_t> type_af_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_af_block;
    {
      Set<hybrid> a;
      type_af_block = NULL;
      if (Ttype->head) {
        a = Ttype->head->set;
        type_af_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(Ttype->head);
        type_af_block->init_pointers(0, &output_buffer, a.cardinality,
                                     a_encoding->num_distinct,
                                     a.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, a.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> f;
        if (Ttype->head->get_block(a_d)) {
          f = Ttype->head->get_block(a_d)->set;
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
          type_af_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = a_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * a_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      a = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      type_af_block->set = &a;
      output_buffer.roll_back(0, halloc_size - a.number_of_bytes);
    }
    std::cout << type_af_cardinality.evaluate(0) << std::endl;
    //////////NPRR
    TrieBlock<hybrid> *name_ac_block = Tname->head;
    //////////NPRR
    par::reducer<size_t> worksFor_ab_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *worksFor_ab_block;
    {
      Set<hybrid> a;
      worksFor_ab_block = NULL;
      if (TworksFor->head && emailAddress_ad_block && telephone_ae_block &&
          type_af_block && name_ac_block) {
        worksFor_ab_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size_a =
            sizeof(uint64_t) * a_encoding->num_distinct * 2;
        a.data =
            output_buffer.get_next(0, alloc_size_a); // initialize the memory
        Set<hybrid> a_tmp(
            tmp_buffer.get_next(0, alloc_size_a)); // initialize the memory
        a_tmp = *ops::set_intersect(
                    &a_tmp, (const Set<hybrid> *)&TworksFor->head->set,
                    (const Set<hybrid> *)&emailAddress_ad_block->set);
        a = *ops::set_intersect(&a, (const Set<hybrid> *)&a_tmp,
                                (const Set<hybrid> *)&telephone_ae_block->set);
        a_tmp = *ops::set_intersect(&a_tmp, (const Set<hybrid> *)&a,
                                    (const Set<hybrid> *)&type_af_block->set);
        a = *ops::set_intersect(&a, (const Set<hybrid> *)&a_tmp,
                                (const Set<hybrid> *)&name_ac_block->set);
        tmp_buffer.roll_back(0, alloc_size_a);
        output_buffer.roll_back(0, alloc_size_a - a.number_of_bytes);
        worksFor_ab_block->set = &a;
        worksFor_ab_block->init_pointers(0, &output_buffer, a.cardinality,
                                         a_encoding->num_distinct,
                                         a.type == type::UINTEGER);
      }
      uint32_t *new_head_data =
          (uint32_t *)tmp_buffer.get_next(0, a.cardinality * sizeof(uint32_t));
      std::atomic<size_t> nhd(0);
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> b;
        if (TworksFor->head->get_block(a_d)) {
          b = TworksFor->head->get_block(a_d)->set;
          uint8_t *sd_b = output_buffer.get_next(
              tid, b.cardinality * sizeof(uint32_t)); // initialize the memory
          uint32_t *ob_b = (uint32_t *)output_buffer.get_next(
              tid, b.cardinality * sizeof(uint32_t));
          size_t ob_i_b = 0;
          b.foreach ([&](uint32_t b_data) {
            if (b_data == b_selection)
              ob_b[ob_i_b++] = b_data;
          });
          b = Set<hybrid>::from_array(sd_b, ob_b, ob_i_b);
          output_buffer.roll_back(tid, b.cardinality * sizeof(uint32_t));
        }
        if (b.cardinality != 0) {
          const size_t count = 1;
          worksFor_ab_cardinality.update(tid, count);
          new_head_data[nhd.fetch_add(1)] = a_d;
        }
      });
      const size_t halloc_size =
          sizeof(uint64_t) * a_encoding->num_distinct * 2;
      uint8_t *new_head_mem = output_buffer.get_next(0, halloc_size);
      tbb::parallel_sort(new_head_data, new_head_data + nhd.load());
      a = Set<hybrid>::from_array(new_head_mem, new_head_data, nhd.load());
      worksFor_ab_block->set = &a;
      output_buffer.roll_back(0, halloc_size - a.number_of_bytes);
    }
    std::cout << worksFor_ab_cardinality.evaluate(0) << std::endl;
    ///////////////////TOP DOWN
    par::reducer<size_t> result_acde_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *result_acde_block;
    {
      result_acde_block = worksFor_ab_block;
      if (result_acde_block) {
        result_acde_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(result_acde_block);
        result_acde_block->init_pointers(
            0, &output_buffer, result_acde_block->set.cardinality,
            a_encoding->num_distinct,
            result_acde_block->set.type == type::UINTEGER);
        result_acde_block->set.par_foreach_index([&](size_t tid, uint32_t a_i,
                                                     uint32_t a_d) {
          (void)a_i;
          (void)a_d;
          TrieBlock<hybrid> *c_block = name_ac_block->get_block(a_d);
          TrieBlock<hybrid> *d_block = emailAddress_ad_block->get_block(a_d);
          TrieBlock<hybrid> *e_block = telephone_ae_block->get_block(a_d);
          if (c_block && d_block && e_block) {
            c_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>(c_block);
            result_acde_block->set_block(a_i, a_d, c_block);
            c_block->init_pointers(
                tid, &output_buffer, c_block->set.cardinality,
                c_encoding->num_distinct, c_block->set.type == type::UINTEGER);
            c_block->set.foreach_index([&](uint32_t c_i, uint32_t c_d) {
              (void)c_i;
              (void)c_d;
              if (d_block) {
                d_block =
                    new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(d_block);
                c_block->set_block(c_i, c_d, d_block);
                d_block->init_pointers(tid, &output_buffer,
                                       d_block->set.cardinality,
                                       d_encoding->num_distinct,
                                       d_block->set.type == type::UINTEGER);
                d_block->set.foreach_index([&](uint32_t d_i, uint32_t d_d) {
                  (void)d_i;
                  (void)d_d;
                  if (e_block) {
                    e_block = new (
                        output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                        TrieBlock<hybrid>(e_block);
                    d_block->set_block(d_i, d_d, e_block);
                    const size_t count = e_block->set.cardinality;
                    result_acde_cardinality.update(tid, count);
                  }
                });
              }
            });
          }
        });
      }
      Trie<hybrid> *Tresult = new Trie<hybrid>(result_acde_block);
      tries["result"] = Tresult;
      std::vector<void *> *encodings_result = new std::vector<void *>();
      encodings_result->push_back(a_encoding);
      encodings_result->push_back(c_encoding);
      encodings_result->push_back(d_encoding);
      encodings_result->push_back(e_encoding);
      encodings["result"] = encodings_result;
    }
    std::cout << result_acde_cardinality.evaluate(0) << std::endl;
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
          Tresult->head->get_block(a_d)->set.foreach_index([&](uint32_t c_i,
                                                               uint32_t c_d) {
            (void)c_i;
            if (Tresult->head->get_block(a_d)->get_block(c_d)) {
              Tresult->head->get_block(a_d)->get_block(c_d)->set.foreach_index(
                  [&](uint32_t d_i, uint32_t d_d) {
                    (void)d_i;
                    if (Tresult->head->get_block(a_d)
                            ->get_block(c_d)
                            ->get_block(d_d)) {
                      Tresult->head->get_block(a_d)
                          ->get_block(c_d)
                          ->get_block(d_d)
                          ->set.foreach_index([&](uint32_t e_i, uint32_t e_d) {
                            (void)e_i;
                            std::cout << ((Encoding<std::string> *)
                                              encodings_result->at(0))
                                             ->key_to_value[a_d] << "\t"
                                      << ((Encoding<std::string> *)
                                              encodings_result->at(1))
                                             ->key_to_value[c_d] << "\t"
                                      << ((Encoding<std::string> *)
                                              encodings_result->at(2))
                                             ->key_to_value[d_d] << "\t"
                                      << ((Encoding<std::string> *)
                                              encodings_result->at(3))
                                             ->key_to_value[e_d] << std::endl;
                          });
                    };
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
