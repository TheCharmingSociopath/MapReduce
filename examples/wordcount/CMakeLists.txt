set(target wordcount)

set(sources
    main.cpp
)

set(Boost_USE_STATIC_LIBS OFF) 
set(Boost_USE_MULTITHREADED ON)  
set(Boost_USE_STATIC_RUNTIME OFF) 
find_package(Boost 1.71 COMPONENTS program_options) 

if(NOT Boost_FOUND)
    message (ERROR "Boost libraries are required to build this example.")
endif()

include_directories(${Boost_INCLUDE_DIRS}) 

add_executable(${target}
    ${sources}
)

target_link_libraries(${target}
    PRIVATE
    mapreduce
    ${Boost_LIBRARIES}
)