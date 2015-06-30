set -e
cd duncecap && sbt update && cd ..
if [ ! -d "tests/data" ]; then
  # Control will enter here if $DIRECTORY doesn't exist.
  cd tests && tar -zxvf data.tar.gz && cd ..
fi

./compile.sh queries/lubm/lubm1.datalog

cd ./emptyheaded && make clean && make tests TRAVIS=true 
NAMES=$(find tests -name '*.cpp' -exec basename {} \;)
for name in $NAMES; do
  app=`echo "${name}" | cut -d'.' -f1`
  ./bin/${app}
done
cd ..