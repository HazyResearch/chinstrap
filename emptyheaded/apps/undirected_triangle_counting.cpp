#include "main.hpp"

template<class T, class R>
class undirected_triangle_counting: public application<T,R> {
  void run(){
    std::cout  << "Running" << std::endl;
  }
};

template<class T, class R>
static application<T,R>* init_app(){
  return new undirected_triangle_counting<T,R>(); 
}