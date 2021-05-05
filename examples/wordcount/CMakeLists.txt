set(target wordcount)

set(sources
    main.cpp
)

set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost 1.71 COMPONENTS program_options mpi serialization log REQUIRED)
find_package(MPI REQUIRED)

if(NOT Boost_FOUND)
    message (SEND_ERROR "Boost libraries are required to build this example.")
endif()

include_directories(${Boost_INCLUDE_DIRS} ${MPI_INCLUDE_PATH}) 

add_executable(${target}
    ${sources}
)

target_link_libraries(${target}
    PRIVATE
    mapreduce
    ${Boost_LIBRARIES}
    ${MPI_CXX_LIBRARIES}
)