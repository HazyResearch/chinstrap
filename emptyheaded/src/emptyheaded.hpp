#include "Relation.hpp"
#include "Encoding.hpp"
#include "Trie.hpp"

template<class T, class R>
class application{
  public:
    virtual void run() = 0;
};