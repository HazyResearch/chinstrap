#!/bin/bash

#gtest
cd ./lib/ && unzip gtest-1.7.0.zip 
cd gtest-1.7.0/ && ./configure
cd ../..
make -C ./lib/gtest-1.7.0

#tbb
cd ./lib/ && tar -xvf tbb43_20150424oss_lin.tgz
source tbb43_20150424oss/bin/tbbvars.sh intel64