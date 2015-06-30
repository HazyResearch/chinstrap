sbt update
cd tests && tar -zxvf data.tar.gz && cd .. 
./compile.sh queries/lubm/lubm1.datalog
cd ./emptyheaded && make clean && make tests TRAVIS=true && cd ..