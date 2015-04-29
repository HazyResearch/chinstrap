#!/bin/bash

#gtest
wget https://googletest.googlecode.com/files/gtest-1.7.0.zip && unzip gtest-1.7.0.zip 
cd gtest-1.7.0/ && ./configure
cd ..
make -C ./gtest-1.7.0

#tbb
wget https://www.threadingbuildingblocks.org/sites/default/files/software_releases/linux/tbb43_20150424oss_lin.tgz && tar -xvf tbb43_20150424oss_lin.tgz
source tbb43_20150424oss/bin/tbbvars.sh intel64