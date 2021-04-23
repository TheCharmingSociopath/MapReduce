set(target testexample)

set(sources
    main.cpp
)

add_executable(${target}
    ${sources}
)

target_link_libraries(${target}
    PRIVATE
    mapreduce)