#include "gtest/gtest.h"
#include "undirected_triangle_counting.cpp"

TEST(UNDIRECTED_TRIANGLE_COUNTING, UINTEGER) {
  NUM_THREADS = 4;
  undirected_triangle_counting<uinteger> *myapp = new undirected_triangle_counting<uinteger>();
  myapp->run("tests/data/replicated.tsv");
  EXPECT_EQ((uint64_t)1612010, myapp->result);
}

TEST(UNDIRECTED_TRIANGLE_COUNTING, HYBRID) {
  NUM_THREADS = 4;
  undirected_triangle_counting<hybrid> *myapp = new undirected_triangle_counting<hybrid>();
  myapp->run("tests/data/replicated.tsv");
  EXPECT_EQ((uint64_t)1612010, myapp->result);
}
/*
TEST(UNDIRECTED_TRIANGLE_COUNTING, BLOCK) {
  NUM_THREADS = 4;
  undirected_triangle_counting<block> *myapp = new undirected_triangle_counting<block>();
  myapp->run("tests/data/replicated.tsv");
  EXPECT_EQ((uint64_t)1612010, myapp->result);
}
*/