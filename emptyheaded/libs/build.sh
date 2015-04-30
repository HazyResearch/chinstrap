#!/bin/bash 

#assumes this is called from EH folder
cd libs

#clear out 
rm -rf tbb43*
rm -rf gtest*

#get gtest
wget https://googletest.googlecode.com/files/gtest-1.7.0.zip
unzip gtest-1.7.0.zip 
cd gtest-1.7.0 
./configure
cd ..

#get tbb
make -C ./gtest-1.7.0
wget https://www.threadingbuildingblocks.org/sites/default/files/software_releases/linux/tbb43_20150424oss_lin.tgz 
tar zxvf tbb43_20150424oss_lin.tgz
source tbbvars.sh intel64 

cd ..