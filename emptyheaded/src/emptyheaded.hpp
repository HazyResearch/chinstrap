#include "Relation.hpp"
#include "TransitiveClosure.hpp"
#include "Trie.hpp"

template<class T>
class application{
  public:
    virtual void run(std::string p) = 0;
};