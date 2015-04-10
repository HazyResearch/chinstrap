#include "main.hpp"
//#include <tbb/tbb.h>
#define WRITE_VECTOR 1

template<class T, class R>
class undirected_triangle_counting: public application<T,R> {
  void run(){
    //create the relation (currently a column wise table)
    Relation<uint64_t,uint64_t> *R_ab = new Relation<uint64_t,uint64_t>();

//////////////////////////////////////////////////////////////////////
    //File IO (for a tsv, csv should be roughly the same)
    tsv_reader f_reader("/dfs/scratch0/caberger/datasets/higgs/edgelist/replicated.tsv");
    char *next = f_reader.tsv_get_first();
    R_ab->num_columns = 0;
    while(next != NULL){
      //have to code generate number of attributes here
      //maybe can accomplish with variadic templates? Might be hard.
      R_ab->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab->num_columns++;
    }

//////////////////////////////////////////////////////////////////////
    //Encoding

    //encode A
    std::vector<Column<uint64_t>> *a_attributes = new std::vector<Column<uint64_t>>();
    a_attributes->push_back(R_ab->get<0>());
    a_attributes->push_back(R_ab->get<1>());
    Encoding<uint64_t> *a_encoding = new Encoding<uint64_t>(a_attributes);

//////////////////////////////////////////////////////////////////////
    //Trie building

    //after all encodings are done, set up encoded relation (what is passed into the Trie)
    //You can switch the ordering here to be what you want it to be in the Trie.
    //A mapping will need to be kept in the query compiler so it is known what
    //encoding each level in the Trie should map to.
    auto bt = debug::start_clock();
    std::vector<Column<uint32_t>> *ER_ab = new std::vector<Column<uint32_t>>();
    ER_ab->push_back(a_encoding->encoded.at(0)); //perform filter, selection
    ER_ab->push_back(a_encoding->encoded.at(1));

    //add some sort of lambda to do selections 
    Trie *TR_ab = Trie::build(ER_ab,[&](size_t index){
      return ER_ab->at(0).at(index) < ER_ab->at(1).at(index);
    });
    debug::stop_clock("Build",bt);
//////////////////////////////////////////////////////////////////////
    //Prints the relation
    
    //R(a,b) join T(b,c) join S(a,c)
    //TR_ab = R(a,b) 
    Trie *T_bc = TR_ab; //T(b,c)
    Trie *S_ac = TR_ab; //S(a,c)

    //allocate memory
    allocator::memory<uint8_t> B_buffer(R_ab->num_columns);
    allocator::memory<uint8_t> C_buffer(R_ab->num_columns);
    par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){
      return a + b;
    });
    
    auto qt = debug::start_clock();

    const std::unordered_map<uint32_t,Block*> map = TR_ab->head->map;
    const Set<uinteger> A = TR_ab->head->data;
    A.par_foreach([&](size_t tid, uint32_t a_i){
      Set<uinteger> B(B_buffer.get_memory(tid)); //initialize the memory
      //B = ops::set_intersect(&B,&TR_ab->head->map.at(a_i)->data,&T_bc->head->data); //intersect the B
      const Set<uinteger> op1 = map.at(a_i)->data;

      op1.foreach([&](uint32_t b_i){ //Peel off B attributes
        auto iter = map.find(b_i);
        if(iter != map.end()){ //Check that the set is not the empty set
          Set<uinteger> C(C_buffer.get_memory(tid));
          const size_t count = ops::set_intersect(&C,
            &iter->second->data,
            &op1)->cardinality;
          num_triangles.update(tid,count);
        }
      });
    });
    
    size_t result = num_triangles.evaluate(0);
    debug::stop_clock("Query",qt);

    std::cout << result << std::endl;
    
//////////////////////////////////////////////////////////////////////
  }
};

template<class T, class R>
application<T,R>* init_app(){
  return new undirected_triangle_counting<T,R>(); 
}