#define GENERATED
#include "main.hpp"
extern "C" long
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
  long query_result = -1;
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
    EworksFor->push_back(ab_encoding->encoded.at(0));
    encodings_worksFor->push_back((void *)ab_encoding);
    EworksFor->push_back(ab_encoding->encoded.at(1));
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
        1 * 5 * 2 * sizeof(TrieBlock<hybrid>) *
        (d_encoding->num_distinct + ab_encoding->num_distinct +
         ce_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        1 * 5 * 2 * sizeof(TrieBlock<hybrid>) *
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
    par::reducer<size_t> worksFortypesubOrganizationOf_abdce_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *worksFortypesubOrganizationOf_abdce_block;
    {
      Set<hybrid> a;
      worksFortypesubOrganizationOf_abdce_block = NULL;
      if (TworksFor->head && Ttype->head) {
        worksFortypesubOrganizationOf_abdce_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size_a =
            sizeof(uint64_t) * ab_encoding->num_distinct * 2;
        a.data =
            output_buffer.get_next(0, alloc_size_a); // initialize the memory
        a = *ops::set_intersect(&a, (const Set<hybrid> *)&TworksFor->head->set,
                                (const Set<hybrid> *)&Ttype->head->set);
        output_buffer.roll_back(0, alloc_size_a - a.number_of_bytes);
        worksFortypesubOrganizationOf_abdce_block->set = &a;
        worksFortypesubOrganizationOf_abdce_block->init_pointers(
            0, &output_buffer, a.cardinality, ab_encoding->num_distinct,
            a.type == type::UINTEGER);
      }
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> b;
        TrieBlock<hybrid> *b_block = NULL;
        if (TworksFor->head->get_block(a_d) && TsubOrganizationOf->head &&
            Ttype->head) {
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
                  (const Set<hybrid> *)&TworksFor->head->get_block(a_d)->set,
                  (const Set<hybrid> *)&TsubOrganizationOf->head->set,
                  [&](uint32_t b_data, uint32_t _1, uint32_t _2) {
                    return b_data == b_selection;
                  });
          b = *ops::set_intersect(&b, (const Set<hybrid> *)&b_tmp,
                                  (const Set<hybrid> *)&Ttype->head->set);
          tmp_buffer.roll_back(tid, alloc_size_b);
          output_buffer.roll_back(tid, alloc_size_b - b.number_of_bytes);
          b_block->set = &b;
          b_block->init_pointers(tid, &output_buffer, b.cardinality,
                                 ab_encoding->num_distinct,
                                 b.type == type::UINTEGER);
        }
        bool b_block_valid = false;
        b.foreach_index([&](uint32_t b_i, uint32_t b_d) {
          Set<hybrid> d;
          TrieBlock<hybrid> *d_block = NULL;
          if (TsubOrganizationOf->head->get_block(b_d)) {
            d = TsubOrganizationOf->head->get_block(b_d)->set;
            d_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>(TsubOrganizationOf->head->get_block(b_d));
            d_block->init_pointers(tid, &output_buffer, d.cardinality,
                                   d_encoding->num_distinct,
                                   d.type == type::UINTEGER);
          }
          bool d_block_valid = false;
          d.foreach_index([&](uint32_t d_i, uint32_t d_d) {
            Set<hybrid> c;
            if (Ttype->head->get_block(a_d)) {
              c = Ttype->head->get_block(a_d)->set;
              uint8_t *sd_c = output_buffer.get_next(
                  tid,
                  c.cardinality * sizeof(uint32_t)); // initialize the memory
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
            c.foreach_index([&](uint32_t c_i, uint32_t c_d) {
              Set<hybrid> e;
              if (Ttype->head->get_block(b_d)) {
                e = Ttype->head->get_block(b_d)->set;
                uint8_t *sd_e = output_buffer.get_next(
                    tid,
                    e.cardinality * sizeof(uint32_t)); // initialize the memory
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
                worksFortypesubOrganizationOf_abdce_cardinality.update(tid,
                                                                       count);
                b_block_valid = true;
                d_block_valid = true;
              }
            });
          });
          if (d_block_valid) {
            b_block->set_block(b_i, b_d, d_block);
          } else {
            b_block->set_block(b_i, b_d, NULL);
          }
        });
        if (b_block_valid) {
          worksFortypesubOrganizationOf_abdce_block->set_block(a_i, a_d,
                                                               b_block);
        } else {
          worksFortypesubOrganizationOf_abdce_block->set_block(a_i, a_d, NULL);
        }
      });
    }
    query_result = worksFortypesubOrganizationOf_abdce_cardinality.evaluate(0);
    std::cout << worksFortypesubOrganizationOf_abdce_cardinality.evaluate(0)
              << std::endl;
    Trie<hybrid> *Tresult =
        new Trie<hybrid>(worksFortypesubOrganizationOf_abdce_block);
    tries["result"] = Tresult;
    std::vector<void *> *encodings_result = new std::vector<void *>();
    encodings_result->push_back(ab_encoding);
    encodings_result->push_back(ab_encoding);
    encodings_result->push_back(d_encoding);
    encodings_result->push_back(ce_encoding);
    encodings_result->push_back(ce_encoding);
    encodings["result"] = encodings_result;
    debug::stop_clock("JOIN", join_timer);
    tmp_buffer.free();
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
