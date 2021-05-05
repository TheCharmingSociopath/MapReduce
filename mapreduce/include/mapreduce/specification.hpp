#pragma once

#include <chrono>
#include <cstddef>

namespace MapReduce {
    struct Specifications {
        Specifications()
            : ping_frequency(50),
              ping_failure_time(2000),
              ping_check_frequency(50)
        {    
        }

        std::size_t num_map_workers;
        std::size_t num_reduce_workers;

        bool gather_on_master;
        std::chrono::milliseconds ping_frequency;
        std::chrono::milliseconds ping_failure_time;
        std::chrono::milliseconds ping_check_frequency;
    };
}