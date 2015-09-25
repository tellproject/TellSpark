#!/bin/bash
echo "COMPILING C++ LIBRARY IN ${PWD}/lib"
cd lib

#rm -r *
cmake ../src/main/
make

printf '\nPress [ENTER] to continue compiling'
read _

cd ..
echo "COMPILING SCALA CODE"
sbt clean compile
sbt  "run-main ch.ethz.Experimental"

