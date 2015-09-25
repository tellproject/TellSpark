project(testunsafe)
cmake_minimum_required(VERSION 3.0)

#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11 -stdlib=libc++")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

# compile the Java code
find_package(Java REQUIRED)

macro(javah JAVA_TARGET)
    add_custom_target(javah_${JAVA_TARGET} DEPENDS ${JAVA_TARGET}
        COMMAND "${Java_JAVAH_EXECUTABLE}" -jni -d "${CMAKE_CURRENT_BINARY_DIR}" ${ARGN}
        WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_JAVA_TARGET_OUTPUT_DIR}${CMAKE_FILES_DIRECTORY}/${JAVA_TARGET}.dir"
        COMMENT "Updating JNI headers for ${JAVA_TARGET}" VERBATIM)
endmacro(javah)

include(UseJava)

set(CMAKE_JAVA_COMPILE_FLAGS "-source" "1.8" "-target" "1.8")
set(CMAKE_JNI_TARGET TRUE)
add_jar(testunsafe java/ch/ethz/Main.java java/ch/ethz/NativeTester.java java/ch/ethz/Customer.java VERSION 1.0)
javah(testunsafe "ch.ethz.NativeTester")

find_package(JNI REQUIRED)

include_directories(${JNI_INCLUDE_DIRS})
include_directories(${CMAKE_CURRENT_BINARY_DIR})

include_directories(cpp/mockDataGen/include)
set(SOURCES main.cpp cpp/mockDataGen/src/Customer.cpp cpp/mockDataGen/src/DataSource.cpp cpp/mockDataGen/include/Customer.h cpp/mockDataGen/include/DataSource.h)

add_library(cimpl SHARED ${SOURCES})
add_dependencies(cimpl javah_testunsafe)
target_link_libraries(cimpl ${JNI_LIBRARIES})
