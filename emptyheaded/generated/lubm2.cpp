#define GENERATED
#include "main.hpp"
extern "C" long
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
  long query_result = -1;
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
    EsubOrganizationOf->push_back(abc_encoding->encoded.at(2));
    encodings_subOrganizationOf->push_back((void *)abc_encoding);
    EsubOrganizationOf->push_back(abc_encoding->encoded.at(3));
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
    EundegraduateDegreeFrom->push_back(abc_encoding->encoded.at(4));
    encodings_undegraduateDegreeFrom->push_back((void *)abc_encoding);
    EundegraduateDegreeFrom->push_back(abc_encoding->encoded.at(5));
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
        1 * 6 * 2 * sizeof(TrieBlock<hybrid>) *
        (abc_encoding->num_distinct + xyz_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        1 * 6 * 2 * sizeof(TrieBlock<hybrid>) *
        (abc_encoding->num_distinct + xyz_encoding->num_distinct));
    uint32_t x_selection = xyz_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
    uint32_t y_selection = xyz_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department");
    uint32_t z_selection = xyz_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University");
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t>
        memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_cardinality(
            0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *
        memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_block;
    {
      Set<hybrid> a;
      memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_block = NULL;
      if (TmemberOf->head && TundegraduateDegreeFrom->head && Ttype->head) {
        memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size_a =
            sizeof(uint64_t) * abc_encoding->num_distinct * 2;
        a.data =
            output_buffer.get_next(0, alloc_size_a); // initialize the memory
        Set<hybrid> a_tmp(
            tmp_buffer.get_next(0, alloc_size_a)); // initialize the memory
        a_tmp = *ops::set_intersect(
                    &a_tmp, (const Set<hybrid> *)&TmemberOf->head->set,
                    (const Set<hybrid> *)&TundegraduateDegreeFrom->head->set);
        a = *ops::set_intersect(&a, (const Set<hybrid> *)&a_tmp,
                                (const Set<hybrid> *)&Ttype->head->set);
        tmp_buffer.roll_back(0, alloc_size_a);
        output_buffer.roll_back(0, alloc_size_a - a.number_of_bytes);
        memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_block->set =
            &a;
        memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_block
            ->init_pointers(0, &output_buffer, a.cardinality,
                            abc_encoding->num_distinct,
                            a.type == type::UINTEGER);
      }
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> b;
        TrieBlock<hybrid> *b_block = NULL;
        if (TmemberOf->head->get_block(a_d) && TsubOrganizationOf->head &&
            Ttype->head) {
          b_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>();
          const size_t alloc_size_b =
              sizeof(uint64_t) * abc_encoding->num_distinct * 2;
          b.data = output_buffer.get_next(
              tid, alloc_size_b); // initialize the memory
          Set<hybrid> b_tmp(
              tmp_buffer.get_next(tid, alloc_size_b)); // initialize the memory
          b_tmp =
              *ops::set_intersect(
                  &b_tmp,
                  (const Set<hybrid> *)&TmemberOf->head->get_block(a_d)->set,
                  (const Set<hybrid> *)&TsubOrganizationOf->head->set);
          b = *ops::set_intersect(&b, (const Set<hybrid> *)&b_tmp,
                                  (const Set<hybrid> *)&Ttype->head->set);
          tmp_buffer.roll_back(tid, alloc_size_b);
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
          if (TsubOrganizationOf->head->get_block(b_d) &&
              TundegraduateDegreeFrom->head->get_block(a_d) && Ttype->head) {
            c_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>();
            const size_t alloc_size_c =
                sizeof(uint64_t) * abc_encoding->num_distinct * 2;
            c.data = output_buffer.get_next(
                tid, alloc_size_c); // initialize the memory
            Set<hybrid> c_tmp(tmp_buffer.get_next(
                tid, alloc_size_c)); // initialize the memory
            c_tmp =
                *ops::set_intersect(
                    &c_tmp,
                    (const Set<hybrid> *)&TsubOrganizationOf->head->get_block(
                                                                        b_d)
                        ->set,
                    (const Set<hybrid> *)&TundegraduateDegreeFrom->head
                        ->get_block(a_d)
                        ->set);
            c = *ops::set_intersect(&c, (const Set<hybrid> *)&c_tmp,
                                    (const Set<hybrid> *)&Ttype->head->set);
            tmp_buffer.roll_back(tid, alloc_size_c);
            output_buffer.roll_back(tid, alloc_size_c - c.number_of_bytes);
            c_block->set = &c;
            c_block->init_pointers(tid, &output_buffer, c.cardinality,
                                   abc_encoding->num_distinct,
                                   c.type == type::UINTEGER);
          }
          bool c_block_valid = false;
          c.foreach_index([&](uint32_t c_i, uint32_t c_d) {
            Set<hybrid> x;
            if (Ttype->head->get_block(a_d)) {
              x = Ttype->head->get_block(a_d)->set;
              uint8_t *sd_x = output_buffer.get_next(
                  tid,
                  x.cardinality * sizeof(uint32_t)); // initialize the memory
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
            x.foreach_index([&](uint32_t x_i, uint32_t x_d) {
              Set<hybrid> y;
              if (Ttype->head->get_block(b_d)) {
                y = Ttype->head->get_block(b_d)->set;
                uint8_t *sd_y = output_buffer.get_next(
                    tid,
                    y.cardinality * sizeof(uint32_t)); // initialize the memory
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
              y.foreach_index([&](uint32_t y_i, uint32_t y_d) {
                Set<hybrid> z;
                if (Ttype->head->get_block(c_d)) {
                  z = Ttype->head->get_block(c_d)->set;
                  uint8_t *sd_z = output_buffer.get_next(
                      tid, z.cardinality *
                               sizeof(uint32_t)); // initialize the memory
                  uint32_t *ob_z = (uint32_t *)output_buffer.get_next(
                      tid, z.cardinality * sizeof(uint32_t));
                  size_t ob_i_z = 0;
                  z.foreach ([&](uint32_t z_data) {
                    if (z_data == z_selection)
                      ob_z[ob_i_z++] = z_data;
                  });
                  z = Set<hybrid>::from_array(sd_z, ob_z, ob_i_z);
                  output_buffer.roll_back(tid,
                                          z.cardinality * sizeof(uint32_t));
                }
                if (z.cardinality != 0) {
                  const size_t count = 1;
                  memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_cardinality
                      .update(tid, count);
                  b_block_valid = true;
                  c_block_valid = true;
                }
              });
            });
          });
          if (c_block_valid) {
            b_block->set_block(b_i, b_d, c_block);
          } else {
            b_block->set_block(b_i, b_d, NULL);
          }
        });
        if (b_block_valid) {
          memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_block
              ->set_block(a_i, a_d, b_block);
        } else {
          memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_block
              ->set_block(a_i, a_d, NULL);
        }
      });
    }
    query_result =
        memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_cardinality
            .evaluate(0);
    std::cout
        << memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_cardinality
               .evaluate(0) << std::endl;
    Trie<hybrid> *Tresult = new Trie<hybrid>(
        memberOfsubOrganizationOfundegraduateDegreeFromtype_abcxyz_block);
    tries["result"] = Tresult;
    std::vector<void *> *encodings_result = new std::vector<void *>();
    encodings_result->push_back(abc_encoding);
    encodings_result->push_back(abc_encoding);
    encodings_result->push_back(abc_encoding);
    encodings_result->push_back(xyz_encoding);
    encodings_result->push_back(xyz_encoding);
    encodings_result->push_back(xyz_encoding);
    encodings["result"] = encodings_result;
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
            }
          });
        }
      });
    }
  }
  return query_result;
}
#ifndef GOOGLE_TEST
int main() {
  std::unordered_map<std::string, void *> relations;
  std::unordered_map<std::string, Trie<hybrid> *> tries;
  std::unordered_map<std::string, std::vector<void *> *> encodings;
  run(relations, tries, encodings);
}
#endif
