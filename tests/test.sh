set -e
sbt update
if [ ! -d "tests/data" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  cd tests && tar -zxvf data.tar.gz && cd ..
fi
./compile.sh queries/lubm/lubm1.datalog
cd ./emptyheaded && make clean && make tests TRAVIS=true 
for app in "test_lubm1" "test_undirected_triangle_counting" "test_undirected_triangle_listing"; do
  ./bin/${app}
done
cd ..