#include "main.hpp"

template<class T, class R>
class undirected_triangle_counting: public application<T,R> {
  void run(){
    std::cout  << "Running" << std::endl;

    std::vector<type::primitives> *attr_types = new std::vector<type::primitives>();
    attr_types->push_back(type::uint32_t);
    attr_types->push_back(type::uint64_t);
    attr_types->push_back(type::string);

    Relation r = Relation::from_file("hey.txt",type::tsv,attr_types);
    r.print();
  }
};

template<class T, class R>
static application<T,R>* init_app(){
  return new undirected_triangle_counting<T,R>(); 
}