#define WRITE_VECTOR 0

#include "gtest/gtest.h"
#include "undirected_triangle_counting.cpp"

TEST(TEST1, FACEBOOK_TRIANGLES_HYBRID) {
  MutableGraph* inputGraph = MutableGraph::undirectedFromBinary("test/data/replicated.tsv");
  Parser input_data(4,false,0,0,inputGraph,"hybrid");
  undirected_triangle_counting<hybrid,hybrid> triangle_app(input_data);
  triangle_app.run();
  EXPECT_EQ(1612010, triangle_app.result);
}
