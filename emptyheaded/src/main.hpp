#include "emptyheaded.hpp"

template<class T, class R> application<T,R>* init_app();

int main (int argc, char* argv[]) {
  (void) argc; (void) argv;
  application<uinteger,uinteger>* myapp = init_app<uinteger,uinteger>();
  myapp->run();
  return 0;
}