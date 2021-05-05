#pragma once

namespace MapReduce {
    struct Specifications {
        std::size_t num_map_workers;
        std::size_t num_reduce_workers;

        bool gather_on_master;
    };
}