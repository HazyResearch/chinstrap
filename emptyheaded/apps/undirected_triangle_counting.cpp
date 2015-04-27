#include "main.hpp"

template<class T>
class undirected_triangle_counting: public application<T> {
  void run(std::string path){
    //create the relation (currently a column wise table)
    Relation<uint64_t,uint64_t> *R_ab = new Relation<uint64_t,uint64_t>();

//////////////////////////////////////////////////////////////////////
    //File IO (for a tsv, csv should be roughly the same)
    auto rt = debug::start_clock();
    //tsv_reader f_reader("simple.txt");
    tsv_reader f_reader(path);
    char *next = f_reader.tsv_get_first();
    R_ab->num_rows = 0;
    while(next != NULL){
      //have to code generate number of attributes here
      //maybe can accomplish with variadic templates? Might be hard.
      R_ab->get<0>().append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab->get<1>().append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab->num_rows++;
    }
    debug::stop_clock("Reading File",rt);
//////////////////////////////////////////////////////////////////////
    //Encoding
    auto et = debug::start_clock();
    //encode A
    std::vector<Column<uint64_t>> *a_attributes = new std::vector<Column<uint64_t>>();
    a_attributes->push_back(R_ab->get<0>());
    a_attributes->push_back(R_ab->get<1>());
    Encoding<uint64_t> *a_encoding = new Encoding<uint64_t>(a_attributes);
    debug::stop_clock("Encoding",et);
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
    Trie<T> *TR_ab = Trie<T>::build(ER_ab,[&](size_t index){
      return ER_ab->at(0).at(index) > ER_ab->at(1).at(index);
    });
    
    debug::stop_clock("Build",bt);

//////////////////////////////////////////////////////////////////////
    //Prints the relation    
    //R(a,b) join T(b,c) join S(a,c)

    //allocate memory
    allocator::memory<uint8_t> B_buffer(R_ab->num_rows);
    allocator::memory<uint8_t> C_buffer(R_ab->num_rows);
    par::reducer<size_t> num_triangles(0,[](size_t a, size_t b){
      return a + b;
    });
    
    auto qt = debug::start_clock();

    const Head<T> H = TR_ab->head;
    const Set<T> A = H.data;
    A.par_foreach([&](size_t tid, uint32_t a_i){
      Set<T> B(B_buffer.get_memory(tid)); //initialize the memory
      Set<T> C(C_buffer.get_memory(tid));

      const Set<T> op1 = ((Tail<T>*) H.get_block(a_i))->data;
      //B = ops::set_intersect(&B,&op1,&A); //intersect the B

      op1.foreach([&](uint32_t b_i){ //Peel off B attributes
        const Tail<T>* l2 = (Tail<T>*)H.get_block(b_i);
        if(l2 != NULL){
          const size_t count = ops::set_intersect(&C,
            &l2->data,
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

template<class T>
application<T>* init_app(){
  return new undirected_triangle_counting<T>(); 
}