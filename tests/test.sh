sbt update
if [ ! -d "tests/data" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  cd tests && tar -zxvf data.tar.gz && cd ..
fi
./compile.sh queries/lubm/lubm1.datalog
cd ./emptyheaded && make clean && make tests TRAVIS=true && cd ..