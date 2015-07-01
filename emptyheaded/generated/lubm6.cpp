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
    Relation<std::string, std::string> *type =
        (Relation<std::string, std::string> *)relations["type"];
    std::vector<Column<std::string>> *a_attributes =
        new std::vector<Column<std::string>>();
    std::vector<Column<std::string>> *b_attributes =
        new std::vector<Column<std::string>>();
    a_attributes->push_back(type->get<0>());
    b_attributes->push_back(type->get<1>());
    Encoding<std::string> *a_encoding = new Encoding<std::string>(a_attributes);
    Encoding<std::string> *b_encoding = new Encoding<std::string>(b_attributes);
    std::vector<Column<uint32_t>> *Etype = new std::vector<Column<uint32_t>>();
    std::vector<void *> *encodings_type = new std::vector<void *>();
    Etype->push_back(a_encoding->encoded.at(0));
    encodings_type->push_back((void *)a_encoding);
    Etype->push_back(b_encoding->encoded.at(0));
    encodings_type->push_back((void *)b_encoding);
    Trie<hybrid> *Ttype = Trie<hybrid>::build(Etype, [&](size_t index) {
      (void)index;
      return true;
    });
    tries["type"] = Ttype;
    encodings["type"] = encodings_type;
    allocator::memory<uint8_t> output_buffer(
        1 * 2 * 2 * sizeof(TrieBlock<hybrid>) *
        (a_encoding->num_distinct + b_encoding->num_distinct));
    allocator::memory<uint8_t> tmp_buffer(
        1 * 2 * 2 * sizeof(TrieBlock<hybrid>) *
        (a_encoding->num_distinct + b_encoding->num_distinct));
    uint32_t b_selection =
        b_encoding->value_to_key.at("http://www.lehigh.edu/~zhp2/2004/0401/"
                                    "univ-bench.owl#UndergraduateStudent");
    auto join_timer = debug::start_clock();
    //////////NPRR
    par::reducer<size_t> type_ab_cardinality(
        0, [](size_t a, size_t b) { return a + b; });
    TrieBlock<hybrid> *type_ab_block;
    {
      Set<hybrid> a;
      type_ab_block = NULL;
      if (Ttype->head) {
        a = Ttype->head->set;
        type_ab_block =
            new (output_buffer.get_next(0, sizeof(TrieBlock<hybrid>)))
                TrieBlock<hybrid>(Ttype->head);
        type_ab_block->init_pointers(0, &output_buffer, a.cardinality,
                                     a_encoding->num_distinct,
                                     a.type == type::UINTEGER);
      }
      a.par_foreach_index([&](size_t tid, uint32_t a_i, uint32_t a_d) {
        Set<hybrid> b;
        TrieBlock<hybrid> *b_block = NULL;
        if (Ttype->head->get_block(a_d)) {
          b = Ttype->head->get_block(a_d)->set;
          b_block = new (output_buffer.get_next(tid, sizeof(TrieBlock<hybrid>)))
              TrieBlock<hybrid>(Ttype->head->get_block(a_d));
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
        }
        if (b.cardinality != 0) {
          const size_t count = b.cardinality;
          type_ab_cardinality.update(tid, count);
          type_ab_block->set_block(a_i, a_d, b_block);
        } else {
          type_ab_block->set_block(a_i, a_d, NULL);
        }
      });
    }
    query_result = type_ab_cardinality.evaluate(0);
    std::cout << type_ab_cardinality.evaluate(0) << std::endl;
    Trie<hybrid> *Tresult = new Trie<hybrid>(type_ab_block);
    tries["result"] = Tresult;
    std::vector<void *> *encodings_result = new std::vector<void *>();
    encodings_result->push_back(a_encoding);
    encodings_result->push_back(b_encoding);
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
