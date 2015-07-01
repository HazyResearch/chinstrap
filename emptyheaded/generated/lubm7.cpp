#define GENERATED
#include "main.hpp"
extern "C" long
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
  long query_result = -1;
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
    Relation<std::string, std::string> *teacherOf =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/teacherOf.txt");
    char *next = f_reader.tsv_get_first();
    teacherOf->num_rows = 0;
    while (next != NULL) {
      teacherOf->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      teacherOf->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      teacherOf->num_rows++;
    }
    relations["teacherOf"] = teacherOf;
    std::cout << teacherOf->num_rows << " rows loaded." << std::endl;
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<std::string, std::string> *takesCourse =
        new Relation<std::string, std::string>();
    tsv_reader f_reader(
        "/dfs/scratch0/caberger/systems/lubm/out_data/takesCourse.txt");
    char *next = f_reader.tsv_get_first();
    takesCourse->num_rows = 0;
    while (next != NULL) {
      takesCourse->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      takesCourse->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      takesCourse->num_rows++;
    }
    relations["takesCourse"] = takesCourse;
    std::cout << takesCourse->num_rows << " rows loaded." << std::endl;
  }
  ////////////////////////////////////////////////////////////////////////////////
  {
    Relation<std::string, std::string> *takesCourse =
        (Relation<std::string, std::string> *)relations["takesCourse"];
    Relation<std::string, std::string> *teacherOf =
        (Relation<std::string, std::string> *)relations["teacherOf"];
    Relation<std::string, std::string> *type =
        (Relation<std::string, std::string> *)relations["type"];
    std::vector<Column<std::string>> *a_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *de_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *bc_attributes =
        new std::vector<Column<std::string>>();
    a_attributes->push_back(teacherOf->get<0>());
    de_attributes->push_back(type->get<1>());
    bc_attributes->push_back(teacherOf->get<1>());
    bc_attributes->push_back(takesCourse->get<0>());
    bc_attributes->push_back(takesCourse->get<1>());
    bc_attributes->push_back(type->get<0>());
    Encoding<std::string> *a_encoding = new Encoding<std::string>(a_attributes);
    Encoding<std::string> *de_encoding =
        new Encoding<std::string>(de_attributes);
    Encoding<std::string> *bc_encoding =
        new Encoding<std::string>(bc_attributes);
    std::vector<Column<uint32_t>> *EtakesCourse =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_takesCourse = new std::vector<void *>();
    EtakesCourse->push_back(bc_encoding->encoded.at(2));
    encodings_takesCourse->push_back((void *)bc_encoding);
    EtakesCourse->push_back(bc_encoding->encoded.at(1));
    encodings_takesCourse->push_back((void *)bc_encoding);
    std::vector<Column<uint32_t>> *EteacherOf =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_teacherOf = new std::vector<void *>();
    EteacherOf->push_back(bc_encoding->encoded.at(0));
    encodings_teacherOf->push_back((void *)bc_encoding);
    EteacherOf->push_back(a_encoding->encoded.at(0));
    encodings_teacherOf->push_back((void *)a_encoding);
    std::vector<Column<uint32_t>> *Etype = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_type = new std::vector<void *>();
    Etype->push_back(bc_encoding->encoded.at(3));
    encodings_type->push_back((void *)bc_encoding);
    Etype->push_back(de_encoding->encoded.at(0));
    encodings_type->push_back((void *)de_encoding);
    Trie<hybrid> *TtakesCourse =
        Trie<hybrid>::build(EtakesCourse, [&](size_t index) {
          (void)index;
          return true;
        });
    tries["takesCourse"] = TtakesCourse;
    encodings["takesCourse"] = encodings_takesCourse;
    Trie<hybrid> *TteacherOf =
        Trie<hybrid>::build(EteacherOf, [&](size_t index) {
          (void)index;
          return true;
        });
    tries["teacherOf"] = TteacherOf;
    encodings["teacherOf"] = encodings_teacherOf;
    Trie<hybrid> *Ttype = Trie<hybrid>::build(Etype, [&](size_t index) {
      (void)index;
      return true;
    });
    tries["type"] = Ttype;
    encodings["type"] = encodings_type;
    allocator::memory<uint8_t> output_buffer(
        1 * 5 * 2 * sizeof(TrieBlock<hybrid>) *
        (a_encoding->num_distinct + de_encoding->num_distinct +
         bc_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        1 * 5 * 2 * sizeof(TrieBlock<hybrid>) *
        (a_encoding->num_distinct + de_encoding->num_distinct +
         bc_encoding->num_distinct));
    uint32_t a_selection = a_encoding->value_to_key.at(
        "http://www.Department12.University8.edu/AssociateProfessor2");
    uint32_t d_selection = de_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Course");
    uint32_t e_selection =
        de_encoding->value_to_key.at("http://www.lehigh.edu/~zhp2/2004/0401/"
                                     "univ-bench.owl#UndergraduateStudent");
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> teacherOftakesCoursetype_bcade_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *teacherOftakesCoursetype_bcade_block;
    {
      Set<hybrid> b;
      teacherOftakesCoursetype_bcade_block = NULL;
      if (TteacherOf->head && TtakesCourse->head && Ttype->head) {
        teacherOftakesCoursetype_bcade_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size_b =
            sizeof(uint64_t) * bc_encoding->num_distinct * 2;
        b.data =
            output_buffer.get_next(0, alloc_size_b); // initialize the memory
        Set<hybrid> b_tmp(
            tmp_buffer.get_next(0, alloc_size_b)); // initialize the memory
        b_tmp = *ops::set_intersect(
                    &b_tmp, (const Set<hybrid> *)&TteacherOf->head->set,
                    (const Set<hybrid> *)&TtakesCourse->head->set);
        b = *ops::set_intersect(&b, (const Set<hybrid> *)&b_tmp,
                                (const Set<hybrid> *)&Ttype->head->set);
        tmp_buffer.roll_back(0, alloc_size_b);
        output_buffer.roll_back(0, alloc_size_b - b.number_of_bytes);
        teacherOftakesCoursetype_bcade_block->set = &b;
        teacherOftakesCoursetype_bcade_block->init_pointers(
            0, &output_buffer, b.cardinality, bc_encoding->num_distinct,
            b.type == type::UINTEGER);
      }
      b.par_foreach_index([&](size_t tid, uint32_t b_i, uint32_t b_d) {
        Set<hybrid> c;
        TrieBlock<hybrid> *c_block = NULL;
        if (TtakesCourse->head->get_block(b_d) && Ttype->head) {
          c_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>();
          const size_t alloc_size_c =
              sizeof(uint64_t) * bc_encoding->num_distinct * 2;
          c.data = output_buffer.get_next(
              tid, alloc_size_c); // initialize the memory
          c = *ops::set_intersect(
                  &c,
                  (const Set<hybrid> *)&TtakesCourse->head->get_block(b_d)->set,
                  (const Set<hybrid> *)&Ttype->head->set);
          output_buffer.roll_back(tid, alloc_size_c - c.number_of_bytes);
          c_block->set = &c;
          c_block->init_pointers(tid, &output_buffer, c.cardinality,
                                 bc_encoding->num_distinct,
                                 c.type == type::UINTEGER);
        }
        bool c_block_valid = false;
        c.foreach_index([&](uint32_t c_i, uint32_t c_d) {
          Set<hybrid> a;
          if (TteacherOf->head->get_block(b_d)) {
            a = TteacherOf->head->get_block(b_d)->set;
            uint8_t *sd_a = output_buffer.get_next(
                tid, a.cardinality * sizeof(uint32_t)); // initialize the memory
            uint32_t *ob_a = (uint32_t *)output_buffer.get_next(
                tid, a.cardinality * sizeof(uint32_t));
            size_t ob_i_a = 0;
            a.foreach ([&](uint32_t a_data) {
              if (a_data == a_selection)
                ob_a[ob_i_a++] = a_data;
            });
            a = Set<hybrid>::from_array(sd_a, ob_a, ob_i_a);
            output_buffer.roll_back(tid, a.cardinality * sizeof(uint32_t));
          }
          a.foreach_index([&](uint32_t a_i, uint32_t a_d) {
            Set<hybrid> d;
            if (Ttype->head->get_block(b_d)) {
              d = Ttype->head->get_block(b_d)->set;
              uint8_t *sd_d = output_buffer.get_next(
                  tid,
                  d.cardinality * sizeof(uint32_t)); // initialize the memory
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
            d.foreach_index([&](uint32_t d_i, uint32_t d_d) {
              Set<hybrid> e;
              if (Ttype->head->get_block(c_d)) {
                e = Ttype->head->get_block(c_d)->set;
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
                teacherOftakesCoursetype_bcade_cardinality.update(tid, count);
                c_block_valid = true;
              }
            });
          });
        });
        if (c_block_valid) {
          teacherOftakesCoursetype_bcade_block->set_block(b_i, b_d, c_block);
        } else {
          teacherOftakesCoursetype_bcade_block->set_block(b_i, b_d, NULL);
        }
      });
    }
    query_result = teacherOftakesCoursetype_bcade_cardinality.evaluate(0);
    std::cout << teacherOftakesCoursetype_bcade_cardinality.evaluate(0)
              << std::endl;
    Trie<hybrid> *Tresult =
        new Trie<hybrid>(teacherOftakesCoursetype_bcade_block);
    tries["result"] = Tresult;
    std::vector<void *> *encodings_result = new std::vector<void *>();
    encodings_result->push_back(bc_encoding);
    encodings_result->push_back(bc_encoding);
    encodings_result->push_back(a_encoding);
    encodings_result->push_back(de_encoding);
    encodings_result->push_back(de_encoding);
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
