#!/bin/bash
cd $1
echo $PWD
#rm -r *
cmake ../
make
ls
