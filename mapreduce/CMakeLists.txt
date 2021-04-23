set(target mapreduce)

# file(GLOB_RECURSE inc_files "${CMAKE_CURRENT_SOURCE_DIR}/include/${target}/*.hpp")
# file(GLOB_RECURSE src_files "${CMAKE_CURRENT_SOURCE_DIR}/lib/*.cpp")

add_library(${target} INTERFACE)

target_include_directories(${target}
    INTERFACE
    "${CMAKE_CURRENT_SOURCE_DIR}/include/"
)