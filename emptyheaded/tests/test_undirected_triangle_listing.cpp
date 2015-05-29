#include "gtest/gtest.h"
#include "undirected_triangle_listing.cpp"

TEST(UNDIRECTED_TRIANGLE_LISTING, UINTEGER) {
  NUM_THREADS = 4;
  undirected_triangle_listing<uinteger> *myapp = new undirected_triangle_counting<uinteger>();
  myapp->run("tests/data/replicated.tsv");
  EXPECT_EQ((uint64_t)1612010, myapp->result);
}

TEST(UNDIRECTED_TRIANGLE_LISTING, HYBRID) {
  NUM_THREADS = 4;
  undirected_triangle_listing<hybrid> *myapp = new undirected_triangle_counting<hybrid>();
  myapp->run("tests/data/replicated.tsv");
  EXPECT_EQ((uint64_t)1612010, myapp->result);
}

TEST(UNDIRECTED_TRIANGLE_LISTING, BLOCK) {
  NUM_THREADS = 4;
  undirected_triangle_listing<block> *myapp = new undirected_triangle_counting<blosck>();
  myapp->run("tests/data/replicated.tsv");
  EXPECT_EQ((uint64_t)1612010, myapp->result);
}