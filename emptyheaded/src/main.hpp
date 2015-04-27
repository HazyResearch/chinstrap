#include "emptyheaded.hpp"

template<class T> application<T>* init_app();

typedef hybrid mylayout;

int main (int argc, char* argv[]) {
  (void) argc; (void) argv;
  application<mylayout>* myapp = init_app<mylayout>();
  myapp->run();
  return 0;
}