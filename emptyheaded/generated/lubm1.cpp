#define GENERATED
#include "main.hpp"
extern "C" long
run(std::unordered_map<std::string, void *> &relations,
    std::unordered_map<std::string, Trie<hybrid> *> tries,
    std::unordered_map<std::string, std::vector<void *> *> encodings) {
  long query_result = -1;
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
    Relation<std::string, std::string> *takesCourse =
        (Relation<std::string, std::string> *)relations["takesCourse"];
    Relation<std::string, std::string> *type =
        (Relation<std::string, std::string> *)relations["type"];
    std::vector<Column<std::string>> *a_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *b_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *c_attributes =
        new std::vector<Column<std::string>>();
    a_attributes->push_back(takesCourse->get<0>());
    a_attributes->push_back(type->get<0>());
    b_attributes->push_back(takesCourse->get<1>());
    c_attributes->push_back(type->get<1>());
    Encoding<std::string> *a_encoding = new Encoding<std::string>(a_attributes);
    Encoding<std::string> *b_encoding = new Encoding<std::string>(b_attributes);
    Encoding<std::string> *c_encoding = new Encoding<std::string>(c_attributes);
    std::vector<Column<uint32_t>> *EtakesCourse =
        new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_takesCourse = new std::vector<void *>();
    EtakesCourse->push_back(a_encoding->encoded.at(0));
    encodings_takesCourse->push_back((void *)a_encoding);
    EtakesCourse->push_back(b_encoding->encoded.at(0));
    encodings_takesCourse->push_back((void *)b_encoding);
    std::vector<Column<uint32_t>> *Etype = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_type = new std::vector<void *>();
    Etype->push_back(a_encoding->encoded.at(1));
    encodings_type->push_back((void *)a_encoding);
    Etype->push_back(c_encoding->encoded.at(0));
    encodings_type->push_back((void *)c_encoding);
    Trie<hybrid> *TtakesCourse =
        Trie<hybrid>::build(EtakesCourse, [&](size_t index) {
          (void)index;
          return true;
        });
    tries["takesCourse"] = TtakesCourse;
    encodings["takesCourse"] = encodings_takesCourse;
    Trie<hybrid> *Ttype = Trie<hybrid>::build(Etype, [&](size_t index) {
      (void)index;
      return true;
    });
    tries["type"] = Ttype;
    encodings["type"] = encodings_type;
    allocator::memory<uint8_t> output_buffer(
        1 * 3 * 2 * sizeof(TrieBlock<hybrid>) *
        (a_encoding->num_distinct + b_encoding->num_distinct +
         c_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        1 * 3 * 2 * sizeof(TrieBlock<hybrid>) *
        (a_encoding->num_distinct + b_encoding->num_distinct +
         c_encoding->num_distinct));
    uint32_t b_selection = b_encoding->value_to_key.at(
        "http://www.Department12.University8.edu/GraduateCourse1");
    uint32_t c_selection = c_encoding->value_to_key.at(
        "http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent");
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> takesCoursetype_abc_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *takesCoursetype_abc_block;
    {
      Set<hybrid> a;
      takesCoursetype_abc_block = NULL;
      if (TtakesCourse->head && Ttype->head) {
        takesCoursetype_abc_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>();
        const size_t alloc_size_a =
            sizeof(uint64_t) * a_encoding->num_distinct * 2;
        a.data =
            output_buffer.get_next(0, alloc_size_a); // initialize the memory
        a = *ops::set_intersect(&a,
                                (const Set<hybrid> *)&TtakesCourse->head->set,
                                (const Set<hybrid> *)&Ttype->head->set);
        output_buffer.roll_back(0, alloc_size_a - a.number_of_bytes);
        takesCoursetype_abc_block->set = &a;
        takesCoursetype_abc_block->init_pointers(
            0, &output_buffer, a.cardinality, a_encoding->num_distinct,
            a.type == type::UINTEGER);
      }
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> b;
        TrieBlock<hybrid> *b_block = NULL;
        if (TtakesCourse->head->get_block(a_d)) {
          b = TtakesCourse->head->get_block(a_d)->set;
          b_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>(TtakesCourse->head->get_block(a_d));
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
          b_block->set = &b;
          b_block->init_pointers(tid, &output_buffer, b.cardinality,
                                 b_encoding->num_distinct,
                                 b.type == type::UINTEGER);
        }
        bool b_block_valid = false;
        b.foreach_index([&](uint32_t b_i, uint32_t b_d) {
          Set<hybrid> c;
          TrieBlock<hybrid> *c_block = NULL;
          if (Ttype->head->get_block(a_d)) {
            c = Ttype->head->get_block(a_d)->set;
            c_block =
                new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
                    TrieBlock<hybrid>(Ttype->head->get_block(a_d));
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
            c_block->set = &c;
          }
          if (c.cardinality != 0) {
            const size_t count = c.cardinality;
            takesCoursetype_abc_cardinality.update(tid, count);
            b_block_valid = true;
            b_block->set_block(b_i, b_d, c_block);
          } else {
            b_block->set_block(b_i, b_d, NULL);
          }
        });
        if (b_block_valid) {
          takesCoursetype_abc_block->set_block(a_i, a_d, b_block);
        } else {
          takesCoursetype_abc_block->set_block(a_i, a_d, NULL);
        }
      });
    }
    query_result = takesCoursetype_abc_cardinality.evaluate(0);
    std::cout << takesCoursetype_abc_cardinality.evaluate(0) << std::endl;
    Trie<hybrid> *Tresult = new Trie<hybrid>(takesCoursetype_abc_block);
    tries["result"] = Tresult;
    std::vector<void *> *encodings_result = new std::vector<void *>();
    encodings_result->push_back(a_encoding);
    encodings_result->push_back(b_encoding);
    encodings_result->push_back(c_encoding);
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
