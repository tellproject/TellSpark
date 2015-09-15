#!/usr/bin/env bash
# wrapper in java
swig -java example.i
# compiling
gcc -fPIC -c example_wrap.c -I/usr/lib/jvm/java-7-openjdk-amd64/include/ -I/usr/lib/jvm/java-7-openjdk-amd64/include/linux/
gcc -fPIC -c example.c
# linking
ld -G example_wrap.o example.o -o libexample.so
# compiling java
javac *.java
# running java
java runme
# export LD_LIBRARY_PATH
export LD_LIBRARY_PATH=$PWD

# to run this use < . swigRun.sh> to execute the script in the context of the calling shell
