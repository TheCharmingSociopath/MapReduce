cmake_minimum_required(VERSION 3.16 FATAL_ERROR)

# project information
set(PROJECT_NAME "MapReduce")

project(${PROJECT_NAME} CXX)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${PROJECT_BINARY_DIR}/lib)

# options
option(BUILD_SHARED_LIBS  "Build shared instead of static libraries." ON)

add_subdirectory(mapreduce)
add_subdirectory(examples)
