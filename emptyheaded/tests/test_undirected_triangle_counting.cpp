#include "gtest/gtest.h"
#include "undirected_triangle_counting.cpp"

TEST(UNDIRECTED_TRIANGLE_COUNTING, FACEBOOK_TRIANGLES_HYBRID) {
  NUM_THREADS = 4;
  undirected_triangle_counting<hybrid> *myapp = new undirected_triangle_counting<hybrid>();
  myapp->run("tests/data/replicated.tsv");
  EXPECT_EQ((uint64_t)1612010, myapp->result);
}
