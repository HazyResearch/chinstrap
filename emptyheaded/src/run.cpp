#include <iostream>
#include <unordered_map>

extern "C" void run(std::unordered_map<std::string, void*>& relations) {
  std::cout << "run was called" << std::endl;
}
