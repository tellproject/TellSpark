project (mockDataGen)
cmake_minimum_required(VERSION 2.8.9)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

include_directories(include)
#set(SOURCES Customer.cpp DataSource.cpp ${PWD}/file1.h ${YOUR_DIRECTORY}/file2.h)
set(SOURCES src/main.cpp src/Customer.cpp src/DataSource.cpp include/Customer.h include/DataSource.h)
#set(SOURCES src/main.cpp src/DataSource.cpp include/DataSource.h)

add_executable(main ${SOURCES})

#add_executable(main main.cpp)