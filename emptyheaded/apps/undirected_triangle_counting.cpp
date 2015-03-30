#include "main.hpp"

template<class T, class R>
class undirected_triangle_counting: public application<T,R> {
  void run(){
    std::cout  << "Running" << std::endl;

    //create the relation (currently a column wise table)
    Relation<uint32_t,uint64_t,std::string> R_ab;

//////////////////////////////////////////////////////////////////////
    //File IO (for a tsv, csv should be roughly the same)
    tsv_reader f_reader("hey.txt");
    char *next = f_reader.tsv_get_first();
    R_ab.num_columns = 0;
    while(next != NULL){
      //have to code generate number of attributes here
      //maybe can accomplish with variadic templates? Might be hard.
      R_ab.get<0>()->append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab.get<1>()->append_from_string(next);
      next = f_reader.tsv_get_next();
      R_ab.get<2>()->append_from_string(next);
      next = f_reader.tsv_get_next();


      R_ab.num_columns++;
    }
//////////////////////////////////////////////////////////////////////

    


//////////////////////////////////////////////////////////////////////
    //Prints the relation
    for(size_t i = 0; i < R_ab.num_columns; i++){
      std::cout << R_ab.get<0>()->at(i) << "\t"
      << R_ab.get<1>()->at(i) << "\t"
      << R_ab.get<2>()->at(i) << "\t"
      << std::endl;
    }
//////////////////////////////////////////////////////////////////////

  }
};

template<class T, class R>
application<T,R>* init_app(){
  return new undirected_triangle_counting<T,R>(); 
}