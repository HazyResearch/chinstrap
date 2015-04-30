#!/bin/bash 

#assumes this is called from EH folder
cd ./emptyheaded
source ./libs/build.sh
source ./libs/tbbvars.sh intel64
make tests
cd ..